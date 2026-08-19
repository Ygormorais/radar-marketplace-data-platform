"""Contratos estruturais das fontes batch, independentes do runtime Spark."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class LogicalType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL_18_2 = "decimal(18,2)"
    DOUBLE = "double"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True)
class ColumnContract:
    name: str
    logical_type: LogicalType
    nullable: bool = True


@dataclass(frozen=True)
class SourceContract:
    name: str
    filename: str
    primary_key: tuple[str, ...]
    columns: tuple[ColumnContract, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if not column.nullable)

    def validate_header(self, observed: list[str]) -> None:
        expected = list(self.column_names)
        if observed != expected:
            missing = sorted(set(expected) - set(observed))
            unexpected = sorted(set(observed) - set(expected))
            raise ValueError(
                f"Header incompatível em {self.filename}; ausentes={missing}; "
                f"inesperadas={unexpected}; ordem_esperada={expected}"
            )


def _column(
    name: str, logical_type: LogicalType = LogicalType.STRING, nullable: bool = True
) -> ColumnContract:
    return ColumnContract(name, logical_type, nullable)


OLIST_CONTRACTS: dict[str, SourceContract] = {
    contract.name: contract
    for contract in (
        SourceContract(
            "customers",
            "olist_customers_dataset.csv",
            ("customer_id",),
            (
                _column("customer_id", nullable=False),
                _column("customer_unique_id", nullable=False),
                _column("customer_zip_code_prefix", LogicalType.INTEGER, False),
                _column("customer_city", nullable=False),
                _column("customer_state", nullable=False),
            ),
        ),
        SourceContract(
            "geolocation",
            "olist_geolocation_dataset.csv",
            ("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"),
            (
                _column("geolocation_zip_code_prefix", LogicalType.INTEGER, False),
                _column("geolocation_lat", LogicalType.DOUBLE, False),
                _column("geolocation_lng", LogicalType.DOUBLE, False),
                _column("geolocation_city", nullable=False),
                _column("geolocation_state", nullable=False),
            ),
        ),
        SourceContract(
            "order_items",
            "olist_order_items_dataset.csv",
            ("order_id", "order_item_id"),
            (
                _column("order_id", nullable=False),
                _column("order_item_id", LogicalType.INTEGER, False),
                _column("product_id", nullable=False),
                _column("seller_id", nullable=False),
                _column("shipping_limit_date", LogicalType.TIMESTAMP, False),
                _column("price", LogicalType.DECIMAL_18_2, False),
                _column("freight_value", LogicalType.DECIMAL_18_2, False),
            ),
        ),
        SourceContract(
            "payments",
            "olist_order_payments_dataset.csv",
            ("order_id", "payment_sequential"),
            (
                _column("order_id", nullable=False),
                _column("payment_sequential", LogicalType.INTEGER, False),
                _column("payment_type", nullable=False),
                _column("payment_installments", LogicalType.INTEGER, False),
                _column("payment_value", LogicalType.DECIMAL_18_2, False),
            ),
        ),
        SourceContract(
            "reviews",
            "olist_order_reviews_dataset.csv",
            ("review_id", "order_id"),
            (
                _column("review_id", nullable=False),
                _column("order_id", nullable=False),
                _column("review_score", LogicalType.INTEGER, False),
                _column("review_comment_title"),
                _column("review_comment_message"),
                _column("review_creation_date", LogicalType.TIMESTAMP, False),
                _column("review_answer_timestamp", LogicalType.TIMESTAMP, False),
            ),
        ),
        SourceContract(
            "orders",
            "olist_orders_dataset.csv",
            ("order_id",),
            (
                _column("order_id", nullable=False),
                _column("customer_id", nullable=False),
                _column("order_status", nullable=False),
                _column("order_purchase_timestamp", LogicalType.TIMESTAMP, False),
                _column("order_approved_at", LogicalType.TIMESTAMP),
                _column("order_delivered_carrier_date", LogicalType.TIMESTAMP),
                _column("order_delivered_customer_date", LogicalType.TIMESTAMP),
                _column("order_estimated_delivery_date", LogicalType.TIMESTAMP, False),
            ),
        ),
        SourceContract(
            "products",
            "olist_products_dataset.csv",
            ("product_id",),
            (
                _column("product_id", nullable=False),
                _column("product_category_name"),
                _column("product_name_lenght", LogicalType.INTEGER),
                _column("product_description_lenght", LogicalType.INTEGER),
                _column("product_photos_qty", LogicalType.INTEGER),
                _column("product_weight_g", LogicalType.INTEGER),
                _column("product_length_cm", LogicalType.INTEGER),
                _column("product_height_cm", LogicalType.INTEGER),
                _column("product_width_cm", LogicalType.INTEGER),
            ),
        ),
        SourceContract(
            "sellers",
            "olist_sellers_dataset.csv",
            ("seller_id",),
            (
                _column("seller_id", nullable=False),
                _column("seller_zip_code_prefix", LogicalType.INTEGER, False),
                _column("seller_city", nullable=False),
                _column("seller_state", nullable=False),
            ),
        ),
        SourceContract(
            "category_translation",
            "product_category_name_translation.csv",
            ("product_category_name",),
            (
                _column("product_category_name", nullable=False),
                _column("product_category_name_english", nullable=False),
            ),
        ),
    )
}


def get_source_contract(name: str) -> SourceContract:
    try:
        return OLIST_CONTRACTS[name]
    except KeyError as exc:
        raise ValueError(f"Fonte Olist desconhecida: {name}") from exc
