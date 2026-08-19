from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from radar.deployment.fabric import (
    FabricApiClient,
    FabricArtifact,
    FabricDeploymentError,
    deploy_artifacts,
    discover_artifacts,
)


def test_discovery_orders_notebooks_before_pipelines(tmp_path: Path) -> None:
    notebook = tmp_path / "nb_load.Notebook"
    pipeline = tmp_path / "pl_master.DataPipeline"
    notebook.mkdir()
    pipeline.mkdir()
    (notebook / "notebook-content.py").write_text("print('ok')", encoding="utf-8")
    (pipeline / "pipeline-content.json").write_text(
        '{"workspace":"{{workspace_id}}","id":"{{item:Notebook:nb_load}}"}',
        encoding="utf-8",
    )

    artifacts = discover_artifacts(tmp_path)

    assert [(item.item_type, item.display_name) for item in artifacts] == [
        ("Notebook", "nb_load"),
        ("DataPipeline", "pl_master"),
    ]


def test_pipeline_definition_resolves_workspace_and_item_ids(tmp_path: Path) -> None:
    pipeline = tmp_path / "pl_master.DataPipeline"
    pipeline.mkdir()
    (pipeline / "pipeline-content.json").write_text(
        '{"workspace":"{{workspace_id}}","id":"{{item:Notebook:nb_load}}"}',
        encoding="utf-8",
    )
    artifact = discover_artifacts(tmp_path)[0]

    definition = artifact.definition(
        workspace_id="workspace-123",
        item_ids={("Notebook", "nb_load"): "notebook-456"},
    )

    decoded = base64.b64decode(definition["parts"][0]["payload"]).decode("utf-8")
    assert json.loads(decoded) == {"workspace": "workspace-123", "id": "notebook-456"}


def test_definition_resolves_semantic_model_and_connection(tmp_path: Path) -> None:
    pipeline = tmp_path / "pl_serving.DataPipeline"
    pipeline.mkdir()
    (pipeline / "pipeline-content.json").write_text(
        '{"dataset":"{{item:SemanticModel:Radar}}","connection":"{{connection:powerbi}}"}',
        encoding="utf-8",
    )
    artifact = discover_artifacts(tmp_path)[0]
    definition = artifact.definition(
        workspace_id="workspace-123",
        item_ids={("SemanticModel", "Radar"): "model-1"},
        variables={"connection:powerbi": "connection-1"},
    )
    decoded = base64.b64decode(definition["parts"][0]["payload"]).decode("utf-8")
    assert json.loads(decoded) == {"dataset": "model-1", "connection": "connection-1"}


def test_unresolved_item_reference_fails_closed(tmp_path: Path) -> None:
    pipeline = tmp_path / "pl_master.DataPipeline"
    pipeline.mkdir()
    (pipeline / "pipeline-content.json").write_text(
        '{"id":"{{item:Notebook:missing}}"}', encoding="utf-8"
    )
    artifact = discover_artifacts(tmp_path)[0]

    with pytest.raises(FabricDeploymentError, match="Referência não resolvida"):
        artifact.definition(workspace_id="workspace-123", item_ids={})


def test_repository_fabric_definitions_compile_in_dry_run() -> None:
    artifacts = discover_artifacts(Path("fabric"))

    plan = deploy_artifacts(
        workspace_id="00000000-0000-0000-0000-000000000000",
        artifacts=artifacts,
        client=None,
        dry_run=True,
    )

    assert len(plan) == 14
    assert plan[-1] == {
        "action": "validate",
        "type": "DataPipeline",
        "name": "pl_streaming_supervisor",
    }


def test_batch_pipeline_enforces_manifest_and_quality_dependencies() -> None:
    pipeline_path = Path("fabric/pipelines/pl_batch_master.DataPipeline/pipeline-content.json")
    properties = json.loads(pipeline_path.read_text(encoding="utf-8"))["properties"]
    activities = {activity["name"]: activity for activity in properties["activities"]}

    assert "landing_manifest_path" in properties["parameters"]
    assert activities["Bronze Olist"]["dependsOn"] == [
        {"activity": "Prepare Run", "dependencyConditions": ["Succeeded"]}
    ]
    assert (
        activities["Bronze Olist"]["typeProperties"]["parameters"]["source_hashes_json"]["value"]
        == "@activity('Prepare Run').output.result.exitValue"
    )
    assert {dependency["activity"] for dependency in activities["Quality Gate"]["dependsOn"]} == {
        "Silver Logistics",
        "Silver Reconciliation",
    }
    assert activities["Quality Gate"]["policy"]["retry"] == 0
    assert activities["Build Gold with dbt"]["dependsOn"] == [
        {"activity": "Quality Gate", "dependencyConditions": ["Succeeded"]}
    ]
    refresh = activities["Refresh Radar Semantic Model"]
    assert refresh["type"] == "PBISemanticModelRefresh"
    assert refresh["dependsOn"] == [
        {"activity": "Build Gold with dbt", "dependencyConditions": ["Succeeded"]}
    ]


def test_streaming_supervisor_waits_for_both_silver_branches() -> None:
    path = Path("fabric/pipelines/pl_streaming_supervisor.DataPipeline/pipeline-content.json")
    properties = json.loads(path.read_text(encoding="utf-8"))["properties"]
    activities = {activity["name"]: activity for activity in properties["activities"]}
    dependencies = activities["Operational Health"]["dependsOn"]
    assert {dependency["activity"] for dependency in dependencies} == {
        "Silver Delivery State",
        "Silver Clickstream Sessions",
    }
    assert activities["Ingest Delivery Events"]["dependsOn"] == []
    assert activities["Ingest Clickstream"]["dependsOn"] == []


def test_api_client_retries_throttling_and_polls_lro(tmp_path: Path) -> None:
    notebook_dir = tmp_path / "nb_load.Notebook"
    notebook_dir.mkdir()
    (notebook_dir / "notebook-content.py").write_text("print('ok')", encoding="utf-8")
    artifact = FabricArtifact(
        directory=notebook_dir,
        display_name="nb_load",
        item_type="Notebook",
        content_path="notebook-content.py",
        definition_format="FabricGitSource",
    )
    calls = {"create": 0, "operation": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/items"):
            calls["create"] += 1
            if calls["create"] == 1:
                return httpx.Response(429, headers={"Retry-After": "0"})
            return httpx.Response(
                202,
                headers={
                    "Location": "https://api.fabric.microsoft.com/v1/operations/op-1",
                    "Retry-After": "0",
                },
            )
        if request.url.path.endswith("/operations/op-1"):
            calls["operation"] += 1
            return httpx.Response(200, json={"status": "Succeeded"})
        raise AssertionError(f"Chamada inesperada: {request.method} {request.url}")

    with FabricApiClient(
        "token",
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as client:
        client.create_item(
            "workspace-123",
            artifact,
            artifact.definition(workspace_id="workspace-123", item_ids={}),
        )

    assert calls == {"create": 2, "operation": 1}


def test_deploy_updates_existing_item_and_creates_missing_pipeline(tmp_path: Path) -> None:
    notebook = tmp_path / "nb_load.Notebook"
    pipeline = tmp_path / "pl_master.DataPipeline"
    notebook.mkdir()
    pipeline.mkdir()
    (notebook / "notebook-content.py").write_text("print('ok')", encoding="utf-8")
    (pipeline / "pipeline-content.json").write_text(
        '{"workspace":"{{workspace_id}}","id":"{{item:Notebook:nb_load}}"}',
        encoding="utf-8",
    )

    class FakeClient:
        def __init__(self) -> None:
            self.items = [{"type": "Notebook", "displayName": "nb_load", "id": "notebook-1"}]
            self.updated: list[str] = []
            self.created: list[str] = []

        def list_items(self, _: str) -> list[dict[str, Any]]:
            return self.items

        def update_item(self, _: str, item_id: str, definition: Any) -> None:
            assert definition["format"] == "FabricGitSource"
            self.updated.append(item_id)

        def create_item(self, _: str, artifact: FabricArtifact, definition: Any) -> None:
            decoded = base64.b64decode(definition["parts"][0]["payload"]).decode("utf-8")
            assert "notebook-1" in decoded
            self.created.append(artifact.display_name)
            self.items.append(
                {
                    "type": artifact.item_type,
                    "displayName": artifact.display_name,
                    "id": "pipeline-1",
                }
            )

    client = FakeClient()
    plan = deploy_artifacts(
        workspace_id="workspace-123",
        artifacts=discover_artifacts(tmp_path),
        client=cast(FabricApiClient, client),
    )

    assert client.updated == ["notebook-1"]
    assert client.created == ["pl_master"]
    assert [entry["action"] for entry in plan] == ["update", "create"]


def test_duplicate_workspace_items_fail_closed(tmp_path: Path) -> None:
    notebook = tmp_path / "nb_load.Notebook"
    notebook.mkdir()
    (notebook / "notebook-content.py").write_text("print('ok')", encoding="utf-8")

    class DuplicateClient:
        def list_items(self, _: str) -> list[dict[str, str]]:
            item = {"type": "Notebook", "displayName": "nb_load", "id": "notebook-1"}
            return [item, item]

    with pytest.raises(FabricDeploymentError, match="Itens duplicados"):
        deploy_artifacts(
            workspace_id="workspace-123",
            artifacts=discover_artifacts(tmp_path),
            client=cast(FabricApiClient, DuplicateClient()),
        )


def test_missing_required_definition_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "broken.Notebook").mkdir()

    with pytest.raises(FabricDeploymentError, match="Definição obrigatória ausente"):
        discover_artifacts(tmp_path)
