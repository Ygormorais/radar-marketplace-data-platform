"""Extrai um arquivo Olist obtido da fonte oficial e produz manifesto auditável."""

from __future__ import annotations

import argparse
from pathlib import Path

from radar.ingestion.manifest import build_manifest, write_manifest
from radar.ingestion.olist import extract_olist_archive

SOURCE_REFERENCE = "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive", type=Path, required=True, help="ZIP baixado da página da Olist"
    )
    parser.add_argument("--target", type=Path, default=Path("data/raw/olist/v1"))
    args = parser.parse_args()

    extract_olist_archive(args.archive, args.target)
    manifest = build_manifest(
        args.target,
        dataset="olist_brazilian_ecommerce",
        dataset_version="1",
        source_reference=SOURCE_REFERENCE,
    )
    manifest_path = args.target / "_manifest.json"
    write_manifest(manifest, manifest_path)
    print(f"Dataset validado: {len(manifest.files)} arquivos; manifesto: {manifest_path}")


if __name__ == "__main__":
    main()
