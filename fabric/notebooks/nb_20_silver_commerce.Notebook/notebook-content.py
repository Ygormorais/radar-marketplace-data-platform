# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
run_id = "manual"
bronze_root = "Tables"
silver_root = "Tables"

# CELL ********************
from pyspark.sql import functions as F

from radar.silver.merge import apply_scd2, merge_current_state
from radar.silver.transforms import (
    transform_category_translation,
    transform_customers,
    transform_geolocation,
    transform_order_items,
    transform_orders,
    transform_payments,
    transform_products,
    transform_reviews,
    transform_sellers,
)

# CELL ********************
bronze = {
    name: spark.read.format("delta").load(f"{bronze_root}/bronze_{name}")
    for name in (
        "customers",
        "geolocation",
        "order_items",
        "payments",
        "reviews",
        "orders",
        "products",
        "sellers",
        "category_translation",
    )
}

# CELL ********************
customers = transform_customers(bronze["customers"])
sellers = transform_sellers(bronze["sellers"])
products = transform_products(bronze["products"])

scd_jobs = (
    (
        "customers",
        customers.drop("customer_id"),
        ["customer_unique_id"],
        ["zip_code_prefix", "city", "state"],
    ),
    ("sellers", sellers, ["seller_id"], ["zip_code_prefix", "city", "state"]),
    (
        "products",
        products,
        ["product_id"],
        [
            "category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_quantity",
            "weight_g",
            "length_cm",
            "height_cm",
            "width_cm",
        ],
    ),
)
for name, frame, keys, tracked in scd_jobs:
    result = apply_scd2(
        spark,
        frame,
        target_path=f"{silver_root}/silver_{name}_history",
        business_keys=keys,
        tracked_columns=tracked,
        effective_at_column="_ingested_at",
        run_id=run_id,
    )
    if not result.late_arrivals.isEmpty():
        result.late_arrivals.write.format("delta").mode("append").save(
            f"{silver_root}/quarantine_late_{name}"
        )

# CELL ********************
current_jobs = (
    ("customers", customers, ["customer_id"]),
    ("orders", transform_orders(bronze["orders"]), ["order_id"]),
    (
        "order_items",
        transform_order_items(bronze["order_items"]),
        ["order_id", "order_item_id"],
    ),
    (
        "payments",
        transform_payments(bronze["payments"]),
        ["order_id", "payment_sequence"],
    ),
    ("reviews", transform_reviews(bronze["reviews"]), ["review_id", "order_id"]),
    (
        "category_translation",
        transform_category_translation(bronze["category_translation"]),
        ["category_name"],
    ),
)
for name, frame, keys in current_jobs:
    merge_current_state(
        spark,
        frame,
        target_path=f"{silver_root}/silver_{name}",
        business_keys=keys,
        sequence_column="_ingested_at",
    )

# CELL ********************
geography = transform_geolocation(bronze["geolocation"]).withColumn(
    "_effective_at", F.current_timestamp()
)
merge_current_state(
    spark,
    geography,
    target_path=f"{silver_root}/silver_geography",
    business_keys=["zip_code_prefix"],
    sequence_column="_effective_at",
)
