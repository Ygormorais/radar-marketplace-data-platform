"""Validação e extração segura do dataset público da Olist."""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

REQUIRED_OLIST_FILES = frozenset(
    {
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv",
    }
)


def _safe_destination(root: Path, member_name: str) -> Path:
    destination = (root / Path(member_name).name).resolve()
    resolved_root = root.resolve()
    if destination.parent != resolved_root:
        raise ValueError(f"Entrada insegura no arquivo ZIP: {member_name}")
    return destination


def extract_olist_archive(archive: Path, target: Path) -> list[Path]:
    """Extrai apenas os CSVs esperados e rejeita arquivos ausentes/duplicados."""

    target.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    observed: set[str] = set()
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.infolist():
            filename = Path(member.filename).name
            if filename not in REQUIRED_OLIST_FILES:
                continue
            if filename in observed:
                raise ValueError(f"Arquivo duplicado no ZIP: {filename}")
            destination = _safe_destination(target, member.filename)
            with bundle.open(member) as source, destination.open("wb") as output:
                shutil.copyfileobj(source, output)
            observed.add(filename)
            extracted.append(destination)
    missing = REQUIRED_OLIST_FILES - observed
    if missing:
        raise ValueError(f"Dataset Olist incompleto; arquivos ausentes: {sorted(missing)}")
    return sorted(extracted)
