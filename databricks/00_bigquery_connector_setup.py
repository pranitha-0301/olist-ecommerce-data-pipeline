# Databricks Notebook / PySpark Script: 00_bigquery_connector_setup.py
# Phase 3: BigQuery to Databricks Integration Setup
#
# PREREQUISITES IN DATABRICKS:
# 1. Databricks Runtime: 13.3 LTS or 14.3 LTS (Apache Spark 3.4+)
# 2. Installed Cluster Library (Maven):
#    Coordinates: com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0
# 3. Temporary GCS Bucket (for staging Spark-BigQuery data transfers):
#    e.g., gs://your-olist-staging-bucket

import os
import json
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession

# -------------------------------------------------------------------------
# 1. Configuration & Placeholders
# -------------------------------------------------------------------------
# Replace with your actual GCP Project ID, BigQuery Dataset, and Temp GCS Bucket
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"        # e.g., "data-engineering-509307"
BIGQUERY_DATASET = "olist_ecommerce"
TEMP_GCS_BUCKET = "YOUR_TEMP_GCS_BUCKET_NAME"  # e.g., "olist-staging-bucket"

# -------------------------------------------------------------------------
# 2. Authentication setup
# -------------------------------------------------------------------------
# Option A: Read credentials via Databricks Secret Scope (Recommended for production)
# SERVICE_ACCOUNT_KEY_JSON = dbutils.secrets.get(scope="gcp-secrets", key="sa-key-json")

# Option B: Read credentials from environment variable or mounted volume path
SERVICE_ACCOUNT_KEY_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", 
    "/path/to/your-service-account-key.json"
)

# -------------------------------------------------------------------------
# 3. Initialize Spark Session with BigQuery Connector Config
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Olist-BigQuery-to-Databricks") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
    .config("viewsEnabled", "true") \
    .config("materializationDataset", BIGQUERY_DATASET) \
    .getOrCreate()

# Set Global Spark Config for BigQuery Connector
spark.conf.set("parentProject", GCP_PROJECT_ID)
spark.conf.set("project", GCP_PROJECT_ID)
if TEMP_GCS_BUCKET != "YOUR_TEMP_GCS_BUCKET_NAME":
    spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)

print(f"✅ Spark Session initialized. Ready to connect to BigQuery Project: {GCP_PROJECT_ID}")
