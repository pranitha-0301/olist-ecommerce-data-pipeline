# Databricks Notebook / PySpark Script: databricks/gold/01_build_gold_layer.py
# Phase 6: Build Gold Layer Star Schema for E-Commerce BI Analytics

import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession, DataFrame
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import (
    col, lit, concat, expr, date_format, year, month, quarter,
    dayofmonth, dayofweek, when, current_timestamp
)

# -------------------------------------------------------------------------
# 1. Database Configurations
# -------------------------------------------------------------------------
SILVER_DATABASE = "olist_silver_db"
GOLD_DATABASE = "olist_gold_db"


def initialize_gold_database(spark: SparkSession):
    """Creates target Gold database in Databricks metastore if not exists."""
    print(f"Initializing Databricks Database: {GOLD_DATABASE}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
    spark.sql(f"USE {GOLD_DATABASE}")


# -------------------------------------------------------------------------
# 2. Gold Dimension Models
# -------------------------------------------------------------------------

def build_dim_customer(spark: SparkSession) -> DataFrame:
    """Builds dim_customer table from silver_customers."""
    df_silver = spark.table(f"{SILVER_DATABASE}.silver_customers")
    
    df_dim = df_silver.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state"),
        current_timestamp().alias("_gold_processed_at")
    )
    return df_dim


def build_dim_product(spark: SparkSession) -> DataFrame:
    """Builds dim_product table from silver_products."""
    df_silver = spark.table(f"{SILVER_DATABASE}.silver_products")
    
    df_dim = df_silver.select(
        col("product_id"),
        col("product_category_name"),
        col("category_name_english"),
        col("product_name_length"),
        col("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        current_timestamp().alias("_gold_processed_at")
    )
    return df_dim


def build_dim_seller(spark: SparkSession) -> DataFrame:
    """Builds dim_seller table from silver_sellers."""
    df_silver = spark.table(f"{SILVER_DATABASE}.silver_sellers")
    
    df_dim = df_silver.select(
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state"),
        current_timestamp().alias("_gold_processed_at")
    )
    return df_dim


def build_dim_date(spark: SparkSession) -> DataFrame:
    """Generates a complete calendar date dimension spanning 2016-01-01 to 2018-12-31."""
    # Create date range sequence
    df_dates = spark.sql("""
        SELECT explode(sequence(to_date('2016-01-01'), to_date('2018-12-31'), interval 1 day)) as full_date
    """)
    
    df_dim = df_dates.select(
        date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        year(col("full_date")).alias("year"),
        quarter(col("full_date")).alias("quarter"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        dayofmonth(col("full_date")).alias("day"),
        dayofweek(col("full_date")).alias("day_of_week"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        when(dayofweek(col("full_date")).isin(1, 7), 1).otherwise(0).alias("is_weekend")
    )
    return df_dim


def build_dim_location(spark: SparkSession) -> DataFrame:
    """Builds dim_location by combining customer and seller unique locations."""
    df_cust_loc = spark.table(f"{SILVER_DATABASE}.silver_customers") \
        .select(
            col("customer_zip_code_prefix").alias("zip_code_prefix"),
            col("customer_city").alias("city"),
            col("customer_state").alias("state")
        )
        
    df_seller_loc = spark.table(f"{SILVER_DATABASE}.silver_sellers") \
        .select(
            col("seller_zip_code_prefix").alias("zip_code_prefix"),
            col("seller_city").alias("city"),
            col("seller_state").alias("state")
        )
        
    df_dim = df_cust_loc.union(df_seller_loc) \
        .dropDuplicates(["zip_code_prefix", "city", "state"]) \
        .withColumn("_gold_processed_at", current_timestamp())
        
    return df_dim


# -------------------------------------------------------------------------
# 3. Gold Fact Table Model
# -------------------------------------------------------------------------

def build_fact_orders(spark: SparkSession) -> DataFrame:
    """
    Builds fact_orders table at the Grain of 1 row per order item.
    Joins silver_orders with silver_order_items.
    """
    df_orders = spark.table(f"{SILVER_DATABASE}.silver_orders")
    df_order_items = spark.table(f"{SILVER_DATABASE}.silver_order_items")
    
    df_fact = df_order_items.join(df_orders, "order_id", "inner") \
        .select(
            # Primary Key
            concat(col("order_id"), lit("_"), col("order_item_id")).alias("fact_order_item_key"),
            
            # Foreign Keys
            col("order_id"),
            col("order_item_id"),
            col("customer_id"),
            col("product_id"),
            col("seller_id"),
            date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_purchase_date_key"),
            
            # Status & Lifecycle Timestamps
            col("order_status"),
            col("order_purchase_timestamp"),
            col("order_approved_at"),
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date"),
            
            # Fact Measures
            lit(1).alias("quantity"),
            col("price"),
            col("freight_value"),
            col("total_item_value"),
            col("delivery_time_days"),
            col("is_delayed"),
            
            current_timestamp().alias("_gold_processed_at")
        )
        
    return df_fact


# -------------------------------------------------------------------------
# 4. Execution & Pipeline Runner
# -------------------------------------------------------------------------

def run_gold_pipeline(spark: SparkSession):
    """Executes Star Schema builds and writes to Gold Delta Tables."""
    print("==================================================")
    print("STARTING PHASE 6: GOLD LAYER DATA MODELING (STAR SCHEMA)")
    print("==================================================")
    
    initialize_gold_database(spark)
    
    models = {
        "dim_customer": build_dim_customer,
        "dim_product": build_dim_product,
        "dim_seller": build_dim_seller,
        "dim_date": build_dim_date,
        "dim_location": build_dim_location,
        "fact_orders": build_fact_orders,
    }
    
    summary = {}
    for table_name, build_func in models.items():
        print(f"\nBuilding Gold Table '{table_name}'...")
        df_gold = build_func(spark)
        
        target_ref = f"{GOLD_DATABASE}.{table_name}"
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_ref)
            
        cnt = spark.table(target_ref).count()
        summary[table_name] = cnt
        print(f"✅ Table '{target_ref}' created successfully with {cnt:,} rows.")
        
    print("\n==================================================")
    print("GOLD LAYER STAR SCHEMA SUMMARY REPORT")
    print("==================================================")
    for tbl, count in summary.items():
        print(f" Gold Table: {tbl:<20} | Rows: {count:,}")
    print("==================================================")
    print("🎉 PHASE 6 GOLD LAYER COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("Olist-Gold-Layer") \
        .getOrCreate()
        
    run_gold_pipeline(spark_session)
