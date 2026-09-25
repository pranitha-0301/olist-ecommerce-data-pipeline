import os
import sys
import logging
import pandas as pd
# pyrefly: ignore [missing-import]
import kagglehub
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Ensure parent directory is in Python path for importing config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import (
    GCP_PROJECT_ID,
    BIGQUERY_DATASET,
    KAGGLE_DATASET_ID,
    TABLE_NAME_MAPPING,
    DATE_COLUMNS,
    BIGQUERY_SCHEMAS,
)

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def get_bigquery_client() -> bigquery.Client:
    """Initializes and returns a BigQuery Client."""
    logger.info(f"Connecting to Google BigQuery for GCP Project: '{GCP_PROJECT_ID}'...")
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        logger.info("Successfully connected to BigQuery client.")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise


def create_bigquery_dataset_if_not_exists(client: bigquery.Client, dataset_name: str) -> str:
    """Creates BigQuery dataset if it does not exist."""
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"  # Standard US multi-region
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset '{dataset_id}' ready in location '{dataset.location}'.")
        return dataset_id
    except GoogleCloudError as e:
        logger.error(f"Error creating dataset '{dataset_id}': {e}")
        raise


def fetch_kaggle_dataset(dataset_id: str) -> str:
    """Retrieves dataset programmatically from Kaggle API via kagglehub."""
    logger.info(f"Downloading dataset '{dataset_id}' from Kaggle API...")
    try:
        download_path = kagglehub.dataset_download(dataset_id)
        logger.info(f"Successfully downloaded dataset files to: '{download_path}'")
        return download_path
    except Exception as e:
        logger.error(f"Failed to download dataset from Kaggle API: {e}")
        raise


def load_csv_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    file_path: str,
    table_name: str,
) -> int:
    """Reads a CSV file, parses data types, and uploads to BigQuery with explicit schema."""
    logger.info(f"Processing '{table_name}' from CSV: '{os.path.basename(file_path)}'...")

    # 1. Read CSV
    df = pd.read_csv(file_path)
    initial_row_count = len(df)
    logger.info(f"Read {initial_row_count:,} rows for '{table_name}'.")

    # 2. Parse Date/Timestamp columns if present
    if table_name in DATE_COLUMNS:
        for date_col in DATE_COLUMNS[table_name]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # 3. Handle explicit BigQuery schema
    schema = BIGQUERY_SCHEMAS.get(table_name)
    if not schema:
        logger.warning(f"No explicit schema defined for '{table_name}', falling back to auto-detect.")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table idempotently
    )

    table_ref = f"{dataset_id}.{table_name}"

    # 4. Upload DataFrame to BigQuery
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for job completion
        logger.info(f"Successfully loaded '{table_name}' into BigQuery table '{table_ref}'.")

        # 5. Verify BigQuery table row count
        bq_table = client.get_table(table_ref)
        loaded_rows = bq_table.num_rows
        logger.info(f"Row Count Verification -> Local: {initial_row_count:,} | BigQuery: {loaded_rows:,}")
        
        if initial_row_count == loaded_rows:
            logger.info(f"✅ MATCH: '{table_name}' row count verified successfully.")
        else:
            logger.warning(f"⚠️ MISMATCH: '{table_name}' Pandas ({initial_row_count}) != BigQuery ({loaded_rows})")

        return loaded_rows
    except Exception as e:
        logger.error(f"❌ Failed loading table '{table_name}': {e}")
        raise


def run_pipeline():
    """Main pipeline execution function."""
    logger.info("==================================================")
    logger.info("STARTING PHASE 1: KAGGLE TO BIGQUERY INGESTION")
    logger.info("==================================================")

    # Step 1: Connect to BigQuery
    client = get_bigquery_client()

    # Step 2: Ensure Dataset exists
    full_dataset_id = create_bigquery_dataset_if_not_exists(client, BIGQUERY_DATASET)

    # Step 3: Fetch Dataset from Kaggle API
    download_dir = fetch_kaggle_dataset(KAGGLE_DATASET_ID)

    # Step 4: Loop and Load each CSV to BigQuery
    summary_results = {}
    for filename, table_name in TABLE_NAME_MAPPING.items():
        file_path = os.path.join(download_dir, filename)
        if not os.path.exists(file_path):
            logger.error(f"File not found: '{file_path}'. Skipping.")
            continue
        
        row_count = load_csv_to_bigquery(client, full_dataset_id, file_path, table_name)
        summary_results[table_name] = row_count

    logger.info("==================================================")
    logger.info("PHASE 1 INGESTION SUMMARY REPORT")
    logger.info("==================================================")
    for tbl, count in summary_results.items():
        logger.info(f" Table: {tbl:<30} | Rows: {count:,}")
    logger.info("==================================================")
    logger.info("🎉 PHASE 1 COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    run_pipeline()
