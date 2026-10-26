import os
from pathlib import Path

import kagglehub
import pandas as pd

from config.config import KAGGLE_DATASET_ID, TABLE_NAME_MAPPING

file_name = os.getenv("KAGGLE_FILE_NAME", "olist_orders_dataset.csv")
if file_name not in TABLE_NAME_MAPPING:
    supported_files = ", ".join(TABLE_NAME_MAPPING)
    raise ValueError(
        f"Unsupported Kaggle file '{file_name}'. Choose one of: {supported_files}"
    )

dataset_path = Path(kagglehub.dataset_download(KAGGLE_DATASET_ID))
file_path = dataset_path / file_name
if not file_path.is_file():
    raise FileNotFoundError(
        f"Expected dataset file '{file_name}' was not found in '{dataset_path}'."
    )

df = pd.read_csv(file_path)
print(f"Loaded '{file_path}' ({len(df):,} rows).")
print("First 5 records:")
print(df.head())