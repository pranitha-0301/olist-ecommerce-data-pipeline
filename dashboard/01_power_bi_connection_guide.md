# Phase 8: Power BI to Databricks Gold Layer Connection Guide

## 1. Architecture Flow
```text
Google BigQuery (olist_ecommerce)
       │
       ▼  (Databricks PySpark Medallion Pipeline)
Databricks Gold Layer (olist_gold_db)
       │
       ▼  (Azure Databricks Connector via Personal Access Token)
Power BI Desktop (Import Mode into VertiPaq Engine)
       │
       ▼
Star Schema Data Model & DAX Measures
       │
       ▼
Executive E-Commerce Dashboard
```

---

## 2. Step-by-Step Connection Instructions

### Step 1: Retrieve Databricks SQL Endpoint Credentials
1. Open your **Databricks Workspace**.
2. Navigate to **SQL Warehouses** (or Compute -> Cluster -> Advanced Options -> JDBC/ODBC).
3. Click on **Connection Details** and copy:
   - **Server Hostname**: `adb-XXXXXXXXXXXXXXXX.X.azuredatabricks.net` (or `community.cloud.databricks.com`)
   - **HTTP Path**: `sql/1.0/warehouses/XXXXXXXXXXXXXXXX`

### Step 2: Generate Databricks Personal Access Token (PAT)
1. Click your user profile in top-right -> **User Settings**.
2. Click **Developer** / **Access Tokens** -> Click **Generate New Token**.
3. Set comment to `Power BI Olist Connection` and click **Generate**.
4. Copy token secret securely.

### Step 3: Connect Power BI Desktop to Databricks
1. Open **Power BI Desktop**.
2. Click **Get Data** -> Search for **Azure Databricks** (or **Databricks**) -> Click **Connect**.
3. In the setup dialog:
   - **Server Hostname**: Paste copied Hostname.
   - **HTTP Path**: Paste copied HTTP Path.
   - **Data Connectivity Mode**: Select **Import** (Recommended).
4. Click **OK**.
5. When prompted for authentication:
   - Select **Personal Access Token**.
   - Paste your generated PAT Token.
   - Click **Connect**.

### Step 4: Load Gold Star Schema Tables
1. In the Navigator pane, locate database `olist_gold_db`.
2. Check the 6 Gold tables:
   - `fact_orders`
   - `dim_customer`
   - `dim_product`
   - `dim_seller`
   - `dim_date`
   - `dim_location`
3. Click **Load**.

---

## 3. Storage Mode Decision Matrix: Import vs. DirectQuery

| Feature / Criteria | **Import Mode (RECOMMENDED)** | **DirectQuery Mode** |
| :--- | :--- | :--- |
| **Data Location** | Loaded into Power BI's in-memory VertiPaq Engine | Remains in Databricks SQL Warehouse |
| **Query Performance** | ⚡ **Lightning Fast** (Sub-second response) | 🐢 Dependent on Databricks cluster spin-up & execution speed |
| **DAX Support** | ✅ 100% full DAX & Time-Intelligence support | ⚠️ Restricted DAX capabilities |
| **Cost** | 💰 **Low** (No continuous cluster runtime cost) | 💸 **Higher** (Requires running SQL warehouse per user query) |
| **Offline Capability** | ✅ Works offline without internet | ❌ Fails without active Databricks connection |
| **Best Used For** | Datasets < 10 GB (Olist Gold is ~100MB) | Datasets > 100 GB or Real-Time streaming requirements |

---

## 4. Star Schema Relationship Configuration in Power BI Model View

Ensure relationships are set as **1-to-Many (1:N)** with **Single Cross-Filter Direction** (Dimension ➔ Fact):

1. `dim_customer[customer_id]` `(1)` ──► `fact_orders[customer_id]` `(*)`
2. `dim_product[product_id]` `(1)` ──► `fact_orders[product_id]` `(*)`
3. `dim_seller[seller_id]` `(1)` ──► `fact_orders[seller_id]` `(*)`
4. `dim_date[date_key]` `(1)` ──► `fact_orders[order_purchase_date_key]` `(*)`
