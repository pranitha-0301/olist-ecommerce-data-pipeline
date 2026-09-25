import os
import sys
import glob
import logging
from google.cloud import bigquery

# Ensure parent directory is in Python path for importing config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import GCP_PROJECT_ID, BIGQUERY_DATASET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_sql_file(client: bigquery.Client, file_path: str):
    """Reads and executes a SQL file against BigQuery."""
    filename = os.path.basename(file_path)
    logger.info(f"--------------------------------------------------")
    logger.info(f"EXECUTING VALIDATION SCRIPT: {filename}")
    logger.info(f"--------------------------------------------------")

    with open(file_path, "r") as f:
        query_text = f.read()

    # Replace placeholders dynamically
    query_text = query_text.replace("data-engineering-509307.olist_ecommerce", f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}")

    try:
        query_job = client.query(query_text)
        results = query_job.result()
        
        # Convert results to DataFrame for clean formatting
        df = results.to_dataframe()
        
        if df.empty:
            logger.info("Query executed successfully: No rows returned.")
        else:
            print("\n" + df.to_string(index=False) + "\n")
            
        return df
    except Exception as e:
        logger.error(f"Error executing {filename}: {e}")
        raise


def run_all_validations():
    """Finds and executes all SQL files in the sql directory."""
    logger.info("==================================================")
    logger.info(f"STARTING PHASE 2: BIGQUERY VALIDATIONS ({GCP_PROJECT_ID}.{BIGQUERY_DATASET})")
    logger.info("==================================================")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    sql_dir = os.path.dirname(os.path.abspath(__file__))
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))

    if not sql_files:
        logger.warning("No SQL files found in sql directory.")
        return

    for sql_file in sql_files:
        run_sql_file(client, sql_file)

    logger.info("==================================================")
    logger.info("🎉 PHASE 2 VALIDATIONS COMPLETED SUCCESSFULLY!")
    logger.info("==================================================")


if __name__ == "__main__":
    run_all_validations()
