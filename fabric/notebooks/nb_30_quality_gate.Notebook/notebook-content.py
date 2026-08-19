# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
run_id = "manual"
silver_root = "Tables"
quality_results_path = "Tables/ctl_data_quality_results"

# CELL ********************
from radar.quality.expectations import (
    Expectation,
    Rule,
    Severity,
    enforce_quality_gate,
    evaluate_expectations,
    evaluate_referential_integrity,
    persist_results,
)
from radar.silver.business_rules import ORDER_STATUS_VALUES

# CELL ********************
orders = spark.read.format("delta").load(f"{silver_root}/silver_orders")
items = spark.read.format("delta").load(f"{silver_root}/silver_order_items")
payments = spark.read.format("delta").load(f"{silver_root}/silver_payments")
products = (
    spark.read.format("delta")
    .load(f"{silver_root}/silver_products_history")
    .filter("is_current = true")
)
sellers = (
    spark.read.format("delta")
    .load(f"{silver_root}/silver_sellers_history")
    .filter("is_current = true")
)
reconciliation = spark.read.format("delta").load(f"{silver_root}/silver_financial_reconciliation")

# CELL ********************
results = []
results.extend(
    evaluate_expectations(
        orders,
        [
            Expectation("order_id_not_null", "order_id", Rule.NOT_NULL),
            Expectation("order_id_unique", "order_id", Rule.UNIQUE),
            Expectation(
                "valid_order_status",
                "order_status",
                Rule.ACCEPTED_VALUES,
                accepted_values=ORDER_STATUS_VALUES,
            ),
        ],
        run_id=run_id,
        dataset="silver_orders",
    )
)
results.extend(
    evaluate_expectations(
        items,
        [
            Expectation("order_id_not_null", "order_id", Rule.NOT_NULL),
            Expectation(
                "valid_item_amount",
                "has_valid_amount",
                Rule.ACCEPTED_VALUES,
                accepted_values=(True,),
            ),
        ],
        run_id=run_id,
        dataset="silver_order_items",
    )
)
results.append(
    evaluate_referential_integrity(
        items,
        products,
        child_key="product_id",
        parent_key="product_id",
        run_id=run_id,
        dataset="silver_order_items",
        expectation_name="product_fk",
    )
)
results.append(
    evaluate_referential_integrity(
        items,
        sellers,
        child_key="seller_id",
        parent_key="seller_id",
        run_id=run_id,
        dataset="silver_order_items",
        expectation_name="seller_fk",
    )
)
results.extend(
    evaluate_expectations(
        reconciliation,
        [
            Expectation(
                "payment_reconciliation",
                "is_payment_reconciled",
                Rule.ACCEPTED_VALUES,
                severity=Severity.WARN,
                max_failure_rate=0.05,
                accepted_values=(True,),
            )
        ],
        run_id=run_id,
        dataset="silver_financial_reconciliation",
    )
)

# CELL ********************
persist_results(spark, results, quality_results_path)
display(spark.createDataFrame([result.__dict__ for result in results]))
enforce_quality_gate(results)
