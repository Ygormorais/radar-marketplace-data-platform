"""Compilação e deployment idempotente de definições do Microsoft Fabric."""

from __future__ import annotations

import base64
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

FABRIC_API_URL = "https://api.fabric.microsoft.com/v1"
_ITEM_REFERENCE = re.compile(r"\{\{item:([A-Za-z][A-Za-z0-9]*):([^}]+)}}")
_VARIABLE_REFERENCE = re.compile(r"\{\{(connection:[A-Za-z][A-Za-z0-9_-]*)}}")
_UNRESOLVED_TOKEN = re.compile(r"\{\{[^}]+}}")


class FabricDeploymentError(RuntimeError):
    """Falha de contrato, compilação ou chamada à API do Fabric."""


@dataclass(frozen=True)
class FabricArtifact:
    """Artefato versionado no formato de definição aceito pelo Fabric."""

    directory: Path
    display_name: str
    item_type: str
    content_path: str
    definition_format: str | None = None

    def definition(
        self,
        *,
        workspace_id: str,
        item_ids: Mapping[tuple[str, str], str],
        variables: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        source = (self.directory / self.content_path).read_text(encoding="utf-8")
        rendered = _render_references(
            source,
            workspace_id=workspace_id,
            item_ids=item_ids,
            variables=variables or {},
        )
        part = {
            "path": self.content_path,
            "payload": base64.b64encode(rendered.encode("utf-8")).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        definition: dict[str, Any] = {"parts": [part]}
        if self.definition_format:
            definition["format"] = self.definition_format
        return definition


def _render_references(
    content: str,
    *,
    workspace_id: str,
    item_ids: Mapping[tuple[str, str], str],
    variables: Mapping[str, str],
) -> str:
    rendered = content.replace("{{workspace_id}}", workspace_id)

    def replace_item(match: re.Match[str]) -> str:
        key = (match.group(1), match.group(2))
        try:
            return item_ids[key]
        except KeyError as error:
            raise FabricDeploymentError(
                f"Referência não resolvida: tipo={key[0]} nome={key[1]}"
            ) from error

    rendered = _ITEM_REFERENCE.sub(replace_item, rendered)

    def replace_variable(match: re.Match[str]) -> str:
        key = match.group(1)
        try:
            return variables[key]
        except KeyError as error:
            raise FabricDeploymentError(f"Variável de deployment não resolvida: {key}") from error

    rendered = _VARIABLE_REFERENCE.sub(replace_variable, rendered)
    unresolved = sorted(set(_UNRESOLVED_TOKEN.findall(rendered)))
    if unresolved:
        raise FabricDeploymentError(f"Tokens não resolvidos: {', '.join(unresolved)}")
    return rendered


def discover_artifacts(root: Path) -> list[FabricArtifact]:
    """Descobre notebooks antes de pipelines para resolver dependências por nome."""

    specifications = (
        ("*.Notebook", "Notebook", "notebook-content.py", "FabricGitSource"),
        ("*.DataPipeline", "DataPipeline", "pipeline-content.json", None),
    )
    artifacts: list[FabricArtifact] = []
    for pattern, item_type, content_path, definition_format in specifications:
        for directory in sorted(path for path in root.rglob(pattern) if path.is_dir()):
            required = directory / content_path
            if not required.is_file():
                raise FabricDeploymentError(f"Definição obrigatória ausente: {required}")
            artifacts.append(
                FabricArtifact(
                    directory=directory,
                    display_name=directory.name.removesuffix(f".{item_type}"),
                    item_type=item_type,
                    content_path=content_path,
                    definition_format=definition_format,
                )
            )
    return artifacts


class FabricApiClient:
    """Cliente mínimo para Items API, com retry de throttling e polling de LRO."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = FABRIC_API_URL,
        timeout_seconds: float = 60.0,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not token.strip():
            raise ValueError("Token Fabric vazio")
        self._sleep = sleep
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout_seconds,
            transport=transport,
        )

    def __enter__(self) -> FabricApiClient:
        return self

    def __exit__(self, *_: object) -> None:
        self._client.close()

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        response: httpx.Response | None = None
        for attempt in range(5):
            response = self._client.request(method, url, **kwargs)
            if response.status_code != 429 and response.status_code < 500:
                response.raise_for_status()
                return response
            if attempt < 4:
                retry_after = float(response.headers.get("Retry-After", min(2**attempt, 30)))
                self._sleep(retry_after)
        assert response is not None
        response.raise_for_status()
        return response

    def _wait_for_operation(self, response: httpx.Response) -> None:
        if response.status_code != 202:
            return
        location = response.headers.get("Location")
        if not location:
            raise FabricDeploymentError("Operação assíncrona sem header Location")
        for _ in range(120):
            self._sleep(float(response.headers.get("Retry-After", "5")))
            response = self._request("GET", location)
            payload = response.json()
            status = payload.get("status")
            if status == "Succeeded":
                return
            if status in {"Failed", "Cancelled"}:
                raise FabricDeploymentError(
                    f"Operação Fabric terminou com status {status}: {payload}"
                )
        raise FabricDeploymentError("Timeout aguardando operação assíncrona do Fabric")

    def list_items(self, workspace_id: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        url = f"/workspaces/{workspace_id}/items"
        params: dict[str, str] | None = None
        while url:
            response = self._request("GET", url, params=params)
            payload = response.json()
            items.extend(payload.get("value", []))
            url = payload.get("continuationUri", "")
            params = None
        return items

    def create_item(
        self,
        workspace_id: str,
        artifact: FabricArtifact,
        definition: Mapping[str, Any],
    ) -> None:
        response = self._request(
            "POST",
            f"/workspaces/{workspace_id}/items",
            json={
                "displayName": artifact.display_name,
                "type": artifact.item_type,
                "definition": definition,
            },
        )
        self._wait_for_operation(response)

    def update_item(
        self,
        workspace_id: str,
        item_id: str,
        definition: Mapping[str, Any],
    ) -> None:
        response = self._request(
            "POST",
            f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
            json={"definition": definition},
        )
        self._wait_for_operation(response)


def _catalog(items: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], str]:
    catalog: dict[tuple[str, str], str] = {}
    for item in items:
        key = (str(item["type"]), str(item["displayName"]))
        if key in catalog:
            raise FabricDeploymentError(
                f"Itens duplicados no workspace: tipo={key[0]} nome={key[1]}"
            )
        catalog[key] = str(item["id"])
    return catalog


def deploy_artifacts(
    *,
    workspace_id: str,
    artifacts: Sequence[FabricArtifact],
    client: FabricApiClient | None,
    dry_run: bool = False,
    variables: Mapping[str, str] | None = None,
) -> list[dict[str, str]]:
    """Cria ou atualiza artefatos; dry-run valida a estrutura sem acessar o tenant."""

    if dry_run:
        placeholder_ids = {
            (artifact.item_type, artifact.display_name): f"dry-run-{artifact.display_name}"
            for artifact in artifacts
        }
        for artifact in artifacts:
            content = (artifact.directory / artifact.content_path).read_text(encoding="utf-8")
            for match in _ITEM_REFERENCE.finditer(content):
                placeholder_ids.setdefault(
                    (match.group(1), match.group(2)), f"dry-run-{match.group(2)}"
                )
        dry_run_variables = {"connection:powerbi": "dry-run-powerbi-connection"}
        dry_run_variables.update(variables or {})
        validation_plan = []
        for artifact in artifacts:
            artifact.definition(
                workspace_id=workspace_id,
                item_ids=placeholder_ids,
                variables=dry_run_variables,
            )
            validation_plan.append(
                {"action": "validate", "type": artifact.item_type, "name": artifact.display_name}
            )
        return validation_plan

    if client is None:
        raise ValueError("client é obrigatório fora do dry-run")

    catalog = _catalog(client.list_items(workspace_id))
    deployment_plan: list[dict[str, str]] = []
    for artifact in artifacts:
        definition = artifact.definition(
            workspace_id=workspace_id,
            item_ids=catalog,
            variables=variables or {},
        )
        key = (artifact.item_type, artifact.display_name)
        if item_id := catalog.get(key):
            client.update_item(workspace_id, item_id, definition)
            action = "update"
        else:
            client.create_item(workspace_id, artifact, definition)
            refreshed = _catalog(client.list_items(workspace_id))
            try:
                catalog[key] = refreshed[key]
            except KeyError as error:
                raise FabricDeploymentError(
                    f"Item criado não foi localizado: tipo={key[0]} nome={key[1]}"
                ) from error
            action = "create"
        deployment_plan.append(
            {"action": action, "type": artifact.item_type, "name": artifact.display_name}
        )
    return deployment_plan
