# Databricks Notebook / PySpark Script: databricks/bronze/01_ingest_to_bronze.py
# Phase 4: Ingest Raw BigQuery Tables into Databricks Delta Lake Bronze Layer

import os
from pathlib import Path

from dotenv import load_dotenv

# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession, DataFrame
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import current_timestamp, lit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    credentials_file = Path(credentials_path).expanduser()
    if not credentials_file.is_absolute():
        credentials_file = PROJECT_ROOT / credentials_file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_file.resolve())

# -------------------------------------------------------------------------
# 1. Configurations & Target Database Setup
# -------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "data-engineering-509307")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "olist_ecommerce")
DATABRICKS_DATABASE = "olist_bronze_db"
DEMO_DUPLICATE_ORDER_ITEMS = int(os.getenv("BRONZE_DEMO_DUPLICATE_ROWS", "100"))

TABLE_MAPPINGS = {
    "orders": "bronze_orders",
    "order_items": "bronze_order_items",
    "customers": "bronze_customers",
    "products": "bronze_products",
    "payments": "bronze_payments",
    "reviews": "bronze_reviews",
    "sellers": "bronze_sellers",
    "geolocation": "bronze_geolocation",
    "product_category_translation": "bronze_product_category_translation",
}


def initialize_bronze_database(spark: SparkSession):
    """Creates the target Bronze database/schema in Databricks metastore if not exists."""
    print(f"Initializing Databricks Database: {DATABRICKS_DATABASE}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABRICKS_DATABASE}")
    spark.sql(f"USE {DATABRICKS_DATABASE}")


def read_raw_from_bigquery(spark: SparkSession, bq_table_name: str) -> DataFrame:
    """Reads raw table from BigQuery into PySpark DataFrame."""
    full_table_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{bq_table_name}"
    
    df_raw = spark.read \
        .format("bigquery") \
        .option("table", full_table_ref) \
        .load()
        
    return df_raw


def transform_to_bronze(df_raw: DataFrame, source_table: str) -> DataFrame:
    """
    Applies minimal Bronze enrichment:
    - Preserves raw schema and data strictly as-is
    - Appends metadata auditing columns (_ingested_at, _source_system)
    """
    df_bronze = df_raw \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_system", lit(f"bigquery.{BIGQUERY_DATASET}.{source_table}"))
        
    return df_bronze


def add_demo_order_item_duplicates(df_raw: DataFrame) -> DataFrame:
    """Adds tagged duplicate order-item rows for demonstrating Silver deduplication."""
    if DEMO_DUPLICATE_ORDER_ITEMS <= 0:
        return df_raw

    original_rows = df_raw.withColumn("_is_demo_duplicate", lit(False))
    duplicate_rows = df_raw.limit(DEMO_DUPLICATE_ORDER_ITEMS).withColumn(
        "_is_demo_duplicate", lit(True)
    )
    print(
        f"⚠️ Demo mode: adding up to {DEMO_DUPLICATE_ORDER_ITEMS:,} "
        "duplicate order-item rows to Bronze only."
    )
    return original_rows.unionByName(duplicate_rows)


def write_to_delta_bronze(df_bronze: DataFrame, delta_table_name: str):
    """Writes DataFrame to Databricks Delta Lake format."""
    target_table_ref = f"{DATABRICKS_DATABASE}.{delta_table_name}"
    print(f"Writing to Delta Table: {target_table_ref}...")
    
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table_ref)
        
    print(f"✅ Table '{target_table_ref}' written successfully.")


def run_bronze_pipeline(spark: SparkSession):
    """Main execution function to process all 9 Bronze tables."""
    print("==================================================")
    print("STARTING PHASE 4: BRONZE LAYER INGESTION (DELTA LAKE)")
    print("==================================================")
    
    initialize_bronze_database(spark)
    
    summary = {}
    for bq_table, delta_table in TABLE_MAPPINGS.items():
        print(f"\nProcessing '{bq_table}' -> '{delta_table}'...")
        
        # Step 1: Read raw data from BigQuery
        df_raw = read_raw_from_bigquery(spark, bq_table)
        if bq_table == "order_items":
            df_raw = add_demo_order_item_duplicates(df_raw)
        
        # Step 2: Add metadata enrichment
        df_bronze = transform_to_bronze(df_raw, bq_table)
        
        # Step 3: Save as Delta Table
        write_to_delta_bronze(df_bronze, delta_table)
        
        # Step 4: Verify row count
        count = spark.table(f"{DATABRICKS_DATABASE}.{delta_table}").count()
        summary[delta_table] = count
        
    print("\n==================================================")
    print("BRONZE LAYER INGESTION SUMMARY REPORT")
    print("==================================================")
    for tbl, cnt in summary.items():
        print(f" Delta Table: {tbl:<36} | Rows: {cnt:,}")
    print("==================================================")
    print("🎉 PHASE 4 BRONZE LAYER COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    # Spark session auto-initialized in Databricks environment
    spark_session = SparkSession.builder \
        .appName("Olist-Bronze-Layer") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
        .getOrCreate()
        
    run_bronze_pipeline(spark_session)
