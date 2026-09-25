# Phase 9: E-Commerce Power BI Executive Dashboard Specification

## 1. Dashboard Theme & Color Palette
- **Background**: Modern Dark Slate (`#0F172A`) or Clean Enterprise White (`#F8FAFC`)
- **Primary Color**: Deep Indigo (`#4F46E5`)
- **Secondary Accent**: Electric Teal (`#0EA5E9`)
- **Success / Positive**: Emerald Green (`#10B981`)
- **Warning / Alert**: Rose Red / Coral (`#F43F5E`)
- **Typography**: Inter / Segoe UI (Clean Sans-Serif)

---

## 2. Interactive Global Filter Slicers (Top Filter Bar)
Place these slicers horizontally across the top section of every report page:
1. **Date Range Slicer**: `dim_date[full_date]` (Between Slicer / Date Picker)
2. **Customer State Slicer**: `dim_customer[customer_state]` (Dropdown / Searchable)
3. **Product Category Slicer**: `dim_product[category_name_english]` (Dropdown / Multi-Select)
4. **Order Status Slicer**: `fact_orders[order_status]` (Multi-Select Pills)

---

## 3. Executive Report Page Layout & Visual Specifications

### A. Top Executive KPI Cards (Row 1)
| Card Name | DAX Measure Used | Formatting | Target Visual Type |
| :--- | :--- | :--- | :--- |
| **Total Revenue** | `[Total Revenue]` | Currency (`$#,##0.00`) | Card Visual |
| **Total Orders** | `[Total Orders]` | Integer (`#,##0`) | Card Visual |
| **Total Customers** | `[Total Customers]` | Integer (`#,##0`) | Card Visual |
| **Average Order Value (AOV)** | `[Average Order Value]` | Currency (`$#,##0.00`) | Card Visual |
| **Avg Delivery Time** | `[Average Delivery Time (Days)]` | Decimal (`0.0 Days`) | Card Visual |

---

### B. Sales Analysis Section (Row 2 & 3)
1. **Monthly Revenue & Order Trend**:
   - **Visual**: Line and Stacked Column Chart
   - **X-Axis**: `dim_date[year]` -> `dim_date[month_name]`
   - **Column Y-Axis**: `[Total Revenue]`
   - **Line Y-Axis**: `[Total Orders]`
   - **Insight**: Identifies seasonal revenue peaks and monthly order growth.

2. **Revenue by Product Category**:
   - **Visual**: Horizontal Bar Chart
   - **Y-Axis**: `dim_product[category_name_english]`
   - **X-Axis**: `[Total Revenue]`
   - **Sort**: Descending by Revenue

3. **Top 10 Selling Products**:
   - **Visual**: Matrix / Table Visual
   - **Columns**: `dim_product[product_id]`, `dim_product[category_name_english]`, `[Total Items Sold]`, `[Total Revenue]`
   - **Filter**: Top N = 10 by `[Total Revenue]`

4. **Revenue Distribution by Customer State**:
   - **Visual**: Filled Map / Shape Map or Treemap
   - **Location**: `dim_customer[customer_state]`
   - **Color Saturation**: `[Total Revenue]`

---

### C. Customer Analytics Section
1. **Customers by State**:
   - **Visual**: Column Chart
   - **X-Axis**: `dim_customer[customer_state]`
   - **Y-Axis**: `[Total Customers]`

2. **Orders & Revenue per Customer**:
   - **Visual**: Multi-Row KPI Card
   - **Fields**: `[Orders per Customer]`, `[Revenue per Customer]`

---

### D. Logistics & Delivery Performance Section
1. **Average Delivery Time by State**:
   - **Visual**: Bar Chart
   - **Y-Axis**: `dim_customer[customer_state]`
   - **X-Axis**: `[Average Delivery Time (Days)]`

2. **Estimated vs. Actual Delivery Comparison**:
   - **Visual**: Clustered Bar Chart
   - **Category**: `dim_date[month_name]`
   - **Values**: `AVERAGE(fact_orders[delivery_time_days])` vs `AVERAGE(fact_orders[estimated_vs_actual_delivery_days])`

3. **Delayed Orders Volume & Percentage**:
   - **Visual**: Gauge Chart / KPI Card
   - **Value**: `[Delayed Orders Percentage]` (Target < 5%)

---

### E. Customer Reviews & Ratings Section
1. **Average Review Score**:
   - **Visual**: KPI Card with Star Icon
   - **Measure**: `[Average Review Score]` (e.g. `4.15 / 5.0`)

2. **Review Score Distribution**:
   - **Visual**: Donut Chart / Column Chart
   - **Legend**: `silver_reviews[review_score]` (1 to 5 Stars)
   - **Values**: `[Total Reviews]`

3. **Average Review Score by Product Category**:
   - **Visual**: Scatter Plot / Bar Chart
   - **X-Axis**: `[Average Review Score]`
   - **Y-Axis**: `dim_product[category_name_english]`
   - **Size**: `[Total Revenue]`
