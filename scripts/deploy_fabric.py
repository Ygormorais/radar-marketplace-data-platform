"""CLI de deployment idempotente dos notebooks e pipelines do Radar."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from radar.deployment.fabric import FabricApiClient, deploy_artifacts, discover_artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-id", default=os.getenv("FABRIC_WORKSPACE_ID"))
    parser.add_argument("--fabric-root", type=Path, default=Path("fabric"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--powerbi-connection-id", default=os.getenv("FABRIC_POWERBI_CONNECTION_ID")
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.workspace_id:
        raise SystemExit("Informe --workspace-id ou FABRIC_WORKSPACE_ID")

    artifacts = discover_artifacts(args.fabric_root)
    if not artifacts:
        raise SystemExit(f"Nenhum artefato Fabric encontrado em {args.fabric_root}")

    variables = (
        {"connection:powerbi": args.powerbi_connection_id} if args.powerbi_connection_id else {}
    )
    if args.dry_run:
        plan = deploy_artifacts(
            workspace_id=args.workspace_id,
            artifacts=artifacts,
            client=None,
            dry_run=True,
            variables=variables,
        )
    else:
        token = os.getenv("FABRIC_TOKEN")
        if not token:
            raise SystemExit("FABRIC_TOKEN é obrigatório para deployment")
        with FabricApiClient(token) as client:
            plan = deploy_artifacts(
                workspace_id=args.workspace_id,
                artifacts=artifacts,
                client=client,
                variables=variables,
            )

    print(json.dumps(plan, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
