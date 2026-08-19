"""Deployment idempotente de artefatos Microsoft Fabric."""

from radar.deployment.fabric import (
    FabricApiClient,
    FabricArtifact,
    deploy_artifacts,
    discover_artifacts,
)

__all__ = ["FabricApiClient", "FabricArtifact", "deploy_artifacts", "discover_artifacts"]
