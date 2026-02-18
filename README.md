# E-Commerce Data Lakehouse

Production-grade data lakehouse processing 3+ billion e-commerce records using Apache Iceberg on Azure Databricks with complete medallion architecture.

## Architecture

**Medallion Pattern**: Bronze (raw) → Silver (cleaned) → Gold (analytics-ready)

- **Bronze**: 3.05B rows, 57 GB - Raw ingestion with schema validation
- **Silver**: 2.66B rows, 43 GB - Cleaned, deduplicated, merged
- **Gold**: 2.56B rows, 49 GB - Dimensional model (3 dims + 2 facts)

## Tech Stack

- **Storage**: Apache Iceberg, Azure Data Lake Gen2
- **Compute**: Azure Databricks (PySpark)
- **Catalog**: Unity Catalog
- **Language**: Python 3.x

## Data Model

### Bronze Layer
```
order_items    800M rows    3.8 GB
customers      76M rows     5.5 GB
sellers        271M rows    7.3 GB
orders         700M rows    27 GB    (legacy system - UUID keys)
orders_new     1.2B rows    14 GB    (new system - integer keys)
```

### Silver Layer
```
order_items    799M rows    4.0 GB    (0.08% dupes removed)
customers      76M rows     2.0 GB    (1.03% dupes removed, PII masked)
sellers        257M rows    0.7 GB    (5.34% dupes removed)
orders         1.53B rows   36 GB     (merged legacy + new, schema evolution)
```

### Gold Layer (Star Schema)
```
dim_customers         76M rows     1.2 GB
dim_sellers           257M rows    0.8 GB
dim_date              365 rows     29 KB     (2024 calendar)
fact_orders           1.53B rows   31 GB     (grain: 1 order)
fact_sales_summary    700M rows    16 GB     (aggregated metrics)
```

## Key Features

**Reusable Framework**
- `BronzeIngestion` - Configurable CSV → Iceberg pipeline
- `SilverTransformation` - Parallel processing with DQA
- `GoldTransformation` - Dimensional modeling with SCD Type 1/2 support

**Data Quality**
- Sample-based validation (avoids OOM on billion-row tables)
- 22.6M duplicates removed
- Automated null/skew detection

**Performance**
- Iceberg clustering on date columns
- 73% storage reduction (179GB raw → 49GB gold)
- Parallel processing (16-64 partitions)
- Query-optimized fact tables

**Schema Evolution**
- Merged incompatible order systems (UUID vs integer keys)
- Unified schema with source tracking
- Discovered 30.6% duplicate rate in new system

## Project Structure
```
ecom-project/
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   ├── table_config.py      # Central configuration
│   │   └── spark_config.py
│   └── utils/
│       ├── __init__.py
│       ├── bronze_ingestion.py
│       ├── silver_transformation.py
│       ├── gold_transformation.py
│       ├── data_quality_check.py
│       └── logger.py
├── notebooks/
│   ├── bronze/                   # 5 ingestion notebooks
│   ├── silver/                   # 4 transformation notebooks
│   └── gold/                     # 5 dimensional modeling notebooks
└── README.md
```

## Setup

### Prerequisites
- Azure Databricks workspace
- Azure Data Lake Storage Gen2 account
- Unity Catalog enabled
- Databricks Runtime 13.0+ (with Delta/Iceberg support)

### Installation

1. **Clone Repository**
```bash
   git clone https://github.com/YOUR_USERNAME/ecom-project.git
```

2. **Configure Settings**
   
   Update `src/config/table_config.py` with your Azure details:
```python
   # Line 8-9: Your Azure workspace email
   WORKSPACE_USER = "your-email@domain.com"
   
   # Line 18: Your storage account name
   STORAGE_ACCOUNT = "your-storage-account-name"
   
   # Line 23: Your Unity Catalog name (optional, default: "ecommerce")
   CATALOG_NAME = "ecommerce"
```

3. **Create Unity Catalog Schemas**
   
   Run in Databricks SQL or notebook:
```sql
   CREATE CATALOG IF NOT EXISTS ecommerce;
   
   CREATE SCHEMA IF NOT EXISTS ecommerce.bronze
   MANAGED LOCATION 'abfss://bronze@<storage-account>.dfs.core.windows.net';
   
   CREATE SCHEMA IF NOT EXISTS ecommerce.silver
   MANAGED LOCATION 'abfss://silver@<storage-account>.dfs.core.windows.net';
   
   CREATE SCHEMA IF NOT EXISTS ecommerce.gold
   MANAGED LOCATION 'abfss://gold@<storage-account>.dfs.core.windows.net';
```

4. **Upload Source Data**
   
   Place CSV files in ADLS paths matching your `table_config.py`:
   - `{BRONZE_BASE}/order_items/`
   - `{BRONZE_BASE}/customers/`
   - `{BRONZE_BASE}/sellers/`
   - `{BRONZE_BASE}/orders/`
   - `{BRONZE_BASE}/orders_new/`

5. **Run Notebooks in Order**
   
   **Bronze Layer:**
```
   notebooks/bronze/01_ingest_order_items.ipynb
   notebooks/bronze/02_ingest_customers.ipynb
   notebooks/bronze/03_ingest_sellers.ipynb
   notebooks/bronze/04_ingest_orders.ipynb
   notebooks/bronze/05_ingest_orders_new.ipynb
```
   
   **Silver Layer:**
```
   notebooks/silver/01_transform_order_items.ipynb
   notebooks/silver/02_transform_customers.ipynb
   notebooks/silver/03_transform_sellers.ipynb
   notebooks/silver/04a_transform_orders_legacy.ipynb
   notebooks/silver/04b_transform_orders_new_append.ipynb
```
   
   **Gold Layer:**
```
   notebooks/gold/01_dim_customers.ipynb
   notebooks/gold/02_dim_sellers.ipynb
   notebooks/gold/03_dim_date.ipynb
   notebooks/gold/04_fact_orders.ipynb
   notebooks/gold/05_fact_sales_summary.ipynb
```

## Performance Metrics

- **Total Processing Time**: ~10 hours
- **Compression Ratio**: 68% (bronze), 73% overall
- **Deduplication**: 22.6M records removed
- **Largest Table**: 1.53B rows (fact_orders)
- **Cluster Size**: 4 cores, 16 GB RAM

## Cluster Configuration    

**Databricks Cluster Specs:**
- **Driver**: Standard_D4ds_v5 (16 GB Memory, 4 Cores)
- **Runtime**: 17.3 LTS (includes Apache Spark 3.5.0)
- **Features**: Unity Catalog enabled, Photon accelerated
- **Cost**: 2 DBU/hour
- **Autoscaling**: Single-node (driver-only)

**Optimal for:**
- Development and testing
- Tables up to 2B rows
- 16-64 partition parallel processing
- Memory-efficient transformations with Iceberg clustering

**Recommended Upgrades:**
- For production: Multi-node cluster (add 2-4 workers)
- For tables >2B rows: Standard_D8ds_v5 (32 GB, 8 cores)
- For heavy aggregations: Enable auto-scaling (2-8 workers)

## Design Principles

- **DRY**: 70% code reduction via OOP framework
- **SOLID**: Single-responsibility classes
- **Scalable**: Handles billions of rows on small cluster
- **Maintainable**: Add new tables with single config entry
- **Production-ready**: Error handling, logging, validation

## Usage Examples

### Add New Table to Pipeline

1. Define schema in `src/config/table_config.py`
2. Run bronze ingestion:
```python
   BronzeIngestion(
       spark=spark,
       table_display_name="NEW_TABLE",
       source_path=BRONZE_NEW_TABLE,
       target_table=BRONZE_NEW_TABLE_TABLE
   ).run(...)
```

3. Transform to silver (same pattern)
4. Model in gold (dimensional or fact)

### Query Gold Layer
```sql
-- Customer lifetime value
SELECT 
    c.customer_name,
    c.country,
    SUM(f.order_amount) as lifetime_value,
    COUNT(f.order_key) as total_orders
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_key = c.customer_id
WHERE c.country = 'USA'
GROUP BY c.customer_name, c.country
ORDER BY lifetime_value DESC
LIMIT 10;

-- Sales trends by date
SELECT 
    d.year,
    d.month_name,
    COUNT(f.order_key) as order_count,
    SUM(f.order_amount) as revenue
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.order_date_key = d.date_key
WHERE f.source_system = 'legacy'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-dimension`)
3. Commit changes (`git commit -m 'Add new dimension'`)
4. Push to branch (`git push origin feature/new-dimension`)
5. Open Pull Request

## License

MIT License - see LICENSE file for details

## Author

**Vashishth Chhatbar**  
Data Engineer | Azure Databricks | Apache Iceberg

[LinkedIn](https://linkedin.com/in/vchhatbar) | [GitHub](https://github.com/vchhatbar)

## Acknowledgments

- Built following Databricks Medallion Architecture best practices
- Dimensional modeling based on Kimball methodology
- Inspired by dbt (data build tool) design patterns