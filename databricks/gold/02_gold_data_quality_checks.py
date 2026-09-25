# Databricks Notebook / PySpark Script: databricks/gold/02_gold_data_quality_checks.py
# Phase 7: Data Quality Framework & Automated Validation Report for Gold Layer

import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col

GOLD_DATABASE = "olist_gold_db"


def run_data_quality_suite(spark: SparkSession):
    """Executes comprehensive data quality validation checks against Gold tables."""
    print("==================================================")
    print("STARTING PHASE 7: GOLD LAYER DATA QUALITY SUITE")
    print("==================================================")

    spark.sql(f"USE {GOLD_DATABASE}")
    
    df_fact = spark.table(f"{GOLD_DATABASE}.fact_orders")
    df_cust = spark.table(f"{GOLD_DATABASE}.dim_customer")
    df_prod = spark.table(f"{GOLD_DATABASE}.dim_product")
    df_sell = spark.table(f"{GOLD_DATABASE}.dim_seller")
    df_date = spark.table(f"{GOLD_DATABASE}.dim_date")

    total_fact_records = df_fact.count()

    results = []

    # -------------------------------------------------------------------------
    # Test 1: NULL Primary Key Check
    # -------------------------------------------------------------------------
    null_pk_count = df_fact.filter(col("fact_order_item_key").isNull()).count()
    results.append({
        "Test Name": "Primary Key NULL Check (fact_orders)",
        "Total Records": total_fact_records,
        "Failed Records": null_pk_count,
        "Status": "PASS ✅" if null_pk_count == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 2: Duplicate Primary Key Check
    # -------------------------------------------------------------------------
    distinct_pk_count = df_fact.select("fact_order_item_key").distinct().count()
    dup_count = total_fact_records - distinct_pk_count
    results.append({
        "Test Name": "Primary Key Duplicate Check (fact_orders)",
        "Total Records": total_fact_records,
        "Failed Records": dup_count,
        "Status": "PASS ✅" if dup_count == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 3: Foreign Key Referential Integrity -> dim_customer
    # -------------------------------------------------------------------------
    orphan_cust = df_fact.join(df_cust, "customer_id", "left_anti").count()
    results.append({
        "Test Name": "Referential Integrity Check (fact_orders -> dim_customer)",
        "Total Records": total_fact_records,
        "Failed Records": orphan_cust,
        "Status": "PASS ✅" if orphan_cust == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 4: Foreign Key Referential Integrity -> dim_product
    # -------------------------------------------------------------------------
    orphan_prod = df_fact.join(df_prod, "product_id", "left_anti").count()
    results.append({
        "Test Name": "Referential Integrity Check (fact_orders -> dim_product)",
        "Total Records": total_fact_records,
        "Failed Records": orphan_prod,
        "Status": "PASS ✅" if orphan_prod == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 5: Foreign Key Referential Integrity -> dim_seller
    # -------------------------------------------------------------------------
    orphan_sell = df_fact.join(df_sell, "seller_id", "left_anti").count()
    results.append({
        "Test Name": "Referential Integrity Check (fact_orders -> dim_seller)",
        "Total Records": total_fact_records,
        "Failed Records": orphan_sell,
        "Status": "PASS ✅" if orphan_sell == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 6: Foreign Key Referential Integrity -> dim_date
    # -------------------------------------------------------------------------
    orphan_date = df_fact.join(df_date, df_fact.order_purchase_date_key == df_date.date_key, "left_anti").count()
    results.append({
        "Test Name": "Referential Integrity Check (fact_orders -> dim_date)",
        "Total Records": total_fact_records,
        "Failed Records": orphan_date,
        "Status": "PASS ✅" if orphan_date == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 7: Non-Negative Sales Values Check
    # -------------------------------------------------------------------------
    negative_sales = df_fact.filter(
        (col("price") < 0) | (col("freight_value") < 0) | (col("total_item_value") < 0)
    ).count()
    results.append({
        "Test Name": "Non-Negative Sales Values Check (price/freight)",
        "Total Records": total_fact_records,
        "Failed Records": negative_sales,
        "Status": "PASS ✅" if negative_sales == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Test 8: Order Status Validity Check
    # -------------------------------------------------------------------------
    invalid_status = df_fact.filter(col("order_status").isNull() | (col("order_status") == "")).count()
    results.append({
        "Test Name": "Order Status Validity Check",
        "Total Records": total_fact_records,
        "Failed Records": invalid_status,
        "Status": "PASS ✅" if invalid_status == 0 else "FAIL ❌"
    })

    # -------------------------------------------------------------------------
    # Print Formatted Report Table
    # -------------------------------------------------------------------------
    print("\n==========================================================================================")
    print("GOLD LAYER DATA QUALITY VALIDATION REPORT")
    print("==========================================================================================")
    print(f"{'Test Name':<55} | {'Total Records':<15} | {'Failed Records':<15} | {'Status'}")
    print("-" * 102)
    for res in results:
        print(f"{res['Test Name']:<55} | {res['Total Records']:<15,} | {res['Failed Records']:<15,} | {res['Status']}")
    print("==========================================================================================")
    print("🎉 PHASE 7 DATA QUALITY AUDIT COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("Olist-Gold-DataQuality") \
        .getOrCreate()
        
    run_data_quality_suite(spark_session)
