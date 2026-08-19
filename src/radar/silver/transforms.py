"""Transformações Spark de Bronze para entidades Silver conformadas."""

from __future__ import annotations

from typing import Any

from radar.silver.business_rules import ORDER_STATUS_VALUES


def _normalized_text(column: Any) -> Any:
    from pyspark.sql import functions as F

    normalized = F.lower(F.trim(column))
    normalized = F.translate(
        normalized,
        "áàâãäéèêëíìîïóòôõöúùûüç",
        "aaaaaeeeeiiiiooooouuuuc",
    )
    return F.regexp_replace(normalized, r"\s+", " ")


def _technical_columns(dataframe: Any) -> list[Any]:
    from pyspark.sql import functions as F

    available = set(dataframe.columns)
    names = ("_run_id", "_ingested_at", "_source_file_hash", "_record_hash")
    return [F.col(name) for name in names if name in available]


def transform_customers(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    return dataframe.select(
        "customer_id",
        "customer_unique_id",
        F.col("customer_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        _normalized_text(F.col("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
        *_technical_columns(dataframe),
    )


def transform_sellers(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    return dataframe.select(
        "seller_id",
        F.col("seller_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        _normalized_text(F.col("seller_city")).alias("city"),
        F.upper(F.trim("seller_state")).alias("state"),
        *_technical_columns(dataframe),
    )


def transform_products(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    return dataframe.select(
        "product_id",
        F.coalesce(_normalized_text(F.col("product_category_name")), F.lit("unknown")).alias(
            "category_name"
        ),
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_quantity"),
        F.col("product_weight_g").cast("int").alias("weight_g"),
        F.col("product_length_cm").cast("int").alias("length_cm"),
        F.col("product_height_cm").cast("int").alias("height_cm"),
        F.col("product_width_cm").cast("int").alias("width_cm"),
        (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
        .cast("long")
        .alias("volume_cm3"),
        *_technical_columns(dataframe),
    )


def transform_orders(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    status = F.lower(F.trim("order_status"))
    delivered_at = F.col("order_delivered_customer_date")
    estimated_at = F.col("order_estimated_delivery_date")
    purchase_at = F.col("order_purchase_timestamp")
    delay_days = F.datediff(F.to_date(delivered_at), F.to_date(estimated_at))
    return dataframe.select(
        "order_id",
        "customer_id",
        status.alias("order_status"),
        purchase_at.alias("purchased_at"),
        F.col("order_approved_at").alias("approved_at"),
        F.col("order_delivered_carrier_date").alias("handed_to_carrier_at"),
        delivered_at.alias("delivered_at"),
        estimated_at.alias("estimated_delivery_at"),
        ((F.col("order_approved_at").cast("long") - purchase_at.cast("long")) / 3600).alias(
            "approval_lead_hours"
        ),
        ((delivered_at.cast("long") - purchase_at.cast("long")) / 86400).alias(
            "delivery_lead_days"
        ),
        delay_days.alias("delivery_delay_days"),
        F.when(delivered_at.isNull(), F.lit(None).cast("boolean"))
        .otherwise(delay_days <= 0)
        .alias("is_delivered_on_time"),
        status.isin(*ORDER_STATUS_VALUES).alias("is_valid_status"),
        (status == "delivered").alias("is_delivered"),
        status.isin("canceled", "unavailable").alias("is_terminal_failure"),
        *_technical_columns(dataframe),
    )


def transform_order_items(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    price = F.col("price").cast("decimal(18,2)")
    freight = F.col("freight_value").cast("decimal(18,2)")
    return dataframe.select(
        "order_id",
        F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id",
        "seller_id",
        F.col("shipping_limit_date").alias("shipping_limit_at"),
        price.alias("item_amount"),
        freight.alias("freight_amount"),
        (price + freight).cast("decimal(18,2)").alias("gross_amount"),
        ((price >= 0) & (freight >= 0)).alias("has_valid_amount"),
        *_technical_columns(dataframe),
    )


def transform_payments(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    payment_type = _normalized_text(F.col("payment_type"))
    installments = F.col("payment_installments").cast("int")
    amount = F.col("payment_value").cast("decimal(18,2)")
    return dataframe.select(
        "order_id",
        F.col("payment_sequential").cast("int").alias("payment_sequence"),
        payment_type.alias("payment_type"),
        installments.alias("installments"),
        amount.alias("payment_amount"),
        ((amount >= 0) & (installments >= 0)).alias("has_valid_payment"),
        *_technical_columns(dataframe),
    )


def transform_reviews(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    score = F.col("review_score").cast("int")
    return dataframe.select(
        "review_id",
        "order_id",
        score.alias("review_score"),
        F.trim("review_comment_title").alias("review_title"),
        F.trim("review_comment_message").alias("review_message"),
        F.col("review_creation_date").alias("review_created_at"),
        F.col("review_answer_timestamp").alias("review_answered_at"),
        score.between(1, 5).alias("has_valid_score"),
        *_technical_columns(dataframe),
    )


def transform_geolocation(dataframe: Any) -> Any:
    """Conforma um registro por CEP usando mediana para reduzir coordenadas anômalas."""

    from pyspark.sql import functions as F

    normalized = dataframe.select(
        F.col("geolocation_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("latitude"),
        F.col("geolocation_lng").cast("double").alias("longitude"),
        _normalized_text(F.col("geolocation_city")).alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    )
    return normalized.groupBy("zip_code_prefix").agg(
        F.percentile_approx("latitude", 0.5, 10_000).alias("latitude"),
        F.percentile_approx("longitude", 0.5, 10_000).alias("longitude"),
        F.mode("city").alias("city"),
        F.mode("state").alias("state"),
        F.count(F.lit(1)).alias("source_coordinate_count"),
    )


def transform_category_translation(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    return dataframe.select(
        _normalized_text(F.col("product_category_name")).alias("category_name"),
        _normalized_text(F.col("product_category_name_english")).alias("category_name_english"),
        *_technical_columns(dataframe),
    )
