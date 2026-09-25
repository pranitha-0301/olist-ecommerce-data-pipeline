import os
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables from .env file
load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "data-engineering-509307")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "olist_ecommerce")
KAGGLE_DATASET_ID = os.getenv("KAGGLE_DATASET_ID", "olistbr/brazilian-ecommerce")

# File name to BigQuery table name mapping
TABLE_NAME_MAPPING = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}

# Date/Timestamp columns for Pandas conversion before load
DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
}

# Explicit BigQuery Schemas for strong typing
BIGQUERY_SCHEMAS = {
    "customers": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_unique_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_zip_code_prefix", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("customer_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_state", "STRING", mode="NULLABLE"),
    ],
    "geolocation": [
        bigquery.SchemaField("geolocation_zip_code_prefix", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("geolocation_lat", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("geolocation_lng", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("geolocation_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geolocation_state", "STRING", mode="NULLABLE"),
    ],
    "order_items": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_item_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("shipping_limit_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("freight_value", "FLOAT64", mode="NULLABLE"),
    ],
    "payments": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payment_sequential", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("payment_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payment_installments", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("payment_value", "FLOAT64", mode="NULLABLE"),
    ],
    "reviews": [
        bigquery.SchemaField("review_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("order_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("review_score", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("review_comment_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("review_comment_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("review_creation_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("review_answer_timestamp", "TIMESTAMP", mode="NULLABLE"),
    ],
    "orders": [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("order_purchase_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("order_approved_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("order_delivered_carrier_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("order_delivered_customer_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("order_estimated_delivery_date", "TIMESTAMP", mode="NULLABLE"),
    ],
    "products": [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_category_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_name_lenght", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_description_lenght", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_photos_qty", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_weight_g", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_length_cm", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_height_cm", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_width_cm", "FLOAT64", mode="NULLABLE"),
    ],
    "sellers": [
        bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("seller_zip_code_prefix", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("seller_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seller_state", "STRING", mode="NULLABLE"),
    ],
    "product_category_translation": [
        bigquery.SchemaField("product_category_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_category_name_english", "STRING", mode="NULLABLE"),
    ],
}
