# Databricks Notebook / PySpark Script: databricks/silver/01_build_silver_layer.py
# Phase 5: Transform Raw Bronze Delta Tables into Cleaned Silver Delta Layer

import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession, DataFrame
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import (
    col, when, coalesce, lit, trim, upper, initcap,
    datediff, current_timestamp
)

# -------------------------------------------------------------------------
# 1. Database Configurations
# -------------------------------------------------------------------------
BRONZE_DATABASE = "olist_bronze_db"
SILVER_DATABASE = "olist_silver_db"


def initialize_silver_database(spark: SparkSession):
    """Creates target Silver database in Databricks metastore if not exists."""
    print(f"Initializing Databricks Database: {SILVER_DATABASE}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")
    spark.sql(f"USE {SILVER_DATABASE}")


# -------------------------------------------------------------------------
# 2. Silver Transformations
# -------------------------------------------------------------------------

def transform_silver_orders(spark: SparkSession) -> DataFrame:
    """
    Silver Orders Transformation:
    - Deduplicates on order_id
    - Fixes carrier date anomaly (carrier date < purchase date set to NULL)
    - Computes derived delivery metrics (delivery_time_days, delivery_delay_days, is_delayed)
    """
    df_bronze = spark.table(f"{BRONZE_DATABASE}.bronze_orders")
    
    df_silver = df_bronze \
        .dropDuplicates(["order_id"]) \
        .withColumn("order_status", trim(col("order_status"))) \
        .withColumn(
            "order_delivered_carrier_date",
            when(col("order_delivered_carrier_date") < col("order_purchase_timestamp"), None)
            .otherwise(col("order_delivered_carrier_date"))
        ) \
        .withColumn(
            "delivery_time_days",
            datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
        ) \
        .withColumn(
            "estimated_vs_actual_delivery_days",
            datediff(col("order_estimated_delivery_date"), col("order_delivered_customer_date"))
        ) \
        .withColumn(
            "is_delayed",
            when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0)
        ) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


def transform_silver_customers(spark: SparkSession) -> DataFrame:
    """
    Silver Customers Transformation:
    - Deduplicates on customer_id
    - Standardizes city to Title Case and state to Uppercase
    """
    df_bronze = spark.table(f"{BRONZE_DATABASE}.bronze_customers")
    
    df_silver = df_bronze \
        .dropDuplicates(["customer_id"]) \
        .withColumn("customer_city", initcap(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state")))) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


def transform_silver_products(spark: SparkSession) -> DataFrame:
    """
    Silver Products Transformation:
    - Left joins with product_category_translation to add English category names
    - Handles missing category names with 'unassigned'
    - Corrects column typos (product_name_lenght -> product_name_length)
    - Deduplicates on product_id
    """
    df_products = spark.table(f"{BRONZE_DATABASE}.bronze_products")
    df_trans = spark.table(f"{BRONZE_DATABASE}.bronze_product_category_translation")
    
    df_silver = df_products \
        .join(df_trans, "product_category_name", "left") \
        .withColumn("category_name_english", coalesce(trim(col("product_category_name_english")), lit("unassigned"))) \
        .withColumnRenamed("product_name_lenght", "product_name_length") \
        .withColumnRenamed("product_description_lenght", "product_description_length") \
        .dropDuplicates(["product_id"]) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


def transform_silver_sellers(spark: SparkSession) -> DataFrame:
    """
    Silver Sellers Transformation:
    - Deduplicates on seller_id
    - Standardizes seller_city (Title Case) and seller_state (Uppercase)
    """
    df_bronze = spark.table(f"{BRONZE_DATABASE}.bronze_sellers")
    
    df_silver = df_bronze \
        .dropDuplicates(["seller_id"]) \
        .withColumn("seller_city", initcap(trim(col("seller_city")))) \
        .withColumn("seller_state", upper(trim(col("seller_state")))) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


def transform_silver_order_items(spark: SparkSession) -> DataFrame:
    """
    Silver Order Items Transformation:
    - Deduplicates on composite key (order_id, order_item_id)
    - Filters out negative prices / freight values
    - Calculates total_item_value = price + freight_value
    """
    df_bronze = spark.table(f"{BRONZE_DATABASE}.bronze_order_items")
    
    df_silver = df_bronze \
        .dropDuplicates(["order_id", "order_item_id"]) \
        .drop("_is_demo_duplicate") \
        .filter((col("price") >= 0) & (col("freight_value") >= 0)) \
        .withColumn("total_item_value", col("price") + col("freight_value")) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


def transform_silver_payments(spark: SparkSession) -> DataFrame:
    """
    Silver Payments Transformation:
    - Deduplicates on (order_id, payment_sequential)
    - Filters negative payment values
    - Standardizes payment_type
    """
    df_bronze = spark.table(f"{BRONZE_DATABASE}.bronze_payments")
    
    df_silver = df_bronze \
        .dropDuplicates(["order_id", "payment_sequential"]) \
        .filter(col("payment_value") >= 0) \
        .withColumn("payment_type", trim(col("payment_type"))) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


def transform_silver_reviews(spark: SparkSession) -> DataFrame:
    """
    Silver Reviews Transformation:
    - Deduplicates on review_id and order_id
    - Validates review_score between 1 and 5
    - Fills NULL comments with 'No comment'
    """
    df_bronze = spark.table(f"{BRONZE_DATABASE}.bronze_reviews")
    
    df_silver = df_bronze \
        .dropDuplicates(["review_id", "order_id"]) \
        .filter((col("review_score") >= 1) & (col("review_score") <= 5)) \
        .withColumn("review_comment_title", coalesce(trim(col("review_comment_title")), lit("No title"))) \
        .withColumn("review_comment_message", coalesce(trim(col("review_comment_message")), lit("No comment"))) \
        .withColumn("_silver_processed_at", current_timestamp())
        
    return df_silver


# -------------------------------------------------------------------------
# 3. Execution & Pipeline Runner
# -------------------------------------------------------------------------

def run_silver_pipeline(spark: SparkSession):
    """Executes transformations and writes to Silver Delta Tables."""
    print("==================================================")
    print("STARTING PHASE 5: SILVER LAYER TRANSFORMATIONS (DELTA LAKE)")
    print("==================================================")
    
    initialize_silver_database(spark)
    
    transformers = {
        "silver_orders": transform_silver_orders,
        "silver_customers": transform_silver_customers,
        "silver_products": transform_silver_products,
        "silver_sellers": transform_silver_sellers,
        "silver_order_items": transform_silver_order_items,
        "silver_payments": transform_silver_payments,
        "silver_reviews": transform_silver_reviews,
    }
    
    summary = {}
    for silver_table, transform_func in transformers.items():
        print(f"\nBuilding '{silver_table}'...")
        df_silver = transform_func(spark)
        
        target_ref = f"{SILVER_DATABASE}.{silver_table}"
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_ref)
            
        cnt = spark.table(target_ref).count()
        summary[silver_table] = cnt
        print(f"✅ Table '{target_ref}' created successfully with {cnt:,} rows.")
        
    print("\n==================================================")
    print("SILVER LAYER SUMMARY REPORT")
    print("==================================================")
    for tbl, count in summary.items():
        print(f" Silver Table: {tbl:<30} | Rows: {count:,}")
    print("==================================================")
    print("🎉 PHASE 5 SILVER LAYER COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("Olist-Silver-Layer") \
        .getOrCreate()
        
    run_silver_pipeline(spark_session)
