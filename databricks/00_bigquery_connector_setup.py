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
from pathlib import Path

from dotenv import load_dotenv

# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    credentials_file = Path(credentials_path).expanduser()
    if not credentials_file.is_absolute():
        credentials_file = PROJECT_ROOT / credentials_file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_file.resolve())

# -------------------------------------------------------------------------
# 1. Configuration
# -------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "data-engineering-509307")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "olist_ecommerce")
TEMP_GCS_BUCKET = os.getenv("TEMP_GCS_BUCKET")

# -------------------------------------------------------------------------
# 2. Authentication setup
# -------------------------------------------------------------------------
# Uses Application Default Credentials. Set GOOGLE_APPLICATION_CREDENTIALS to a
# service-account key path only when ADC is not configured in the runtime.

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
if TEMP_GCS_BUCKET:
    spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)

print(f"✅ Spark Session initialized. Ready to connect to BigQuery Project: {GCP_PROJECT_ID}")
