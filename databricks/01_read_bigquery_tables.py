# Databricks Notebook / PySpark Script: 01_read_bigquery_tables.py
# Phase 3: Reading all 9 BigQuery Tables into PySpark DataFrames

import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession, DataFrame

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "olist_ecommerce")

TABLE_NAMES = [
    "customers",
    "geolocation",
    "order_items",
    "payments",
    "reviews",
    "orders",
    "products",
    "sellers",
    "product_category_translation",
]


def load_bigquery_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Reads a table from Google BigQuery into a PySpark DataFrame."""
    full_table_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    print(f"Reading BigQuery table: {full_table_ref} ...")
    
    df = spark.read \
        .format("bigquery") \
        .option("table", full_table_ref) \
        .load()
        
    return df


def verify_all_tables(spark: SparkSession):
    """Loops through all 9 tables, reads into PySpark, and verifies row counts."""
    print("==================================================")
    print("DATABRICKS PYSPARK -> BIGQUERY INGESTION VERIFICATION")
    print("==================================================")
    
    dataframes = {}
    for table_name in TABLE_NAMES:
        try:
            df = load_bigquery_table(spark, table_name)
            count = df.count()
            dataframes[table_name] = df
            print(f"  Table: {table_name:<30} | PySpark Rows: {count:,}")
            df.printSchema()
        except Exception as e:
            print(f"❌ Error loading table '{table_name}': {e}")
            
    print("==================================================")
    print("🎉 DATABRICKS BIGQUERY INGESTION COMPLETE!")
    return dataframes


if __name__ == "__main__":
    # In Databricks, 'spark' is automatically initialized
    # If running locally with PySpark installed, initialize spark session:
    spark_session = SparkSession.builder \
        .appName("Databricks-BigQuery-Reader") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
        .getOrCreate()
        
    verify_all_tables(spark_session)
