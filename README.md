# OmniLake: Azure Cloud Data Lakehouse & Analytics Platform

![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Synapse Analytics](https://img.shields.io/badge/Synapse_Analytics-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A4EF?style=for-the-badge&logo=databricks&logoColor=white)

## Team
* **Belal Salah Yaseen** (Team Leader)
* **Samia Youssef Abdelrahman**
* **Mohamed Kamal Gomaa**
* **Youssef Emad Alsaadney**

## Project Overview
**OmniLake** is an end-to-end cloud data platform built on Microsoft Azure. The project solves a common enterprise challenge: transforming fragmented, raw retail data (CSV, JSON) into a scalable, highly structured analytical environment. 

Using the **Medallion Architecture**, this pipeline ingests operational datasets into an Azure Data Lake, processes them using distributed PySpark, and serves a fully modeled Star Schema via Azure Synapse Analytics for downstream business intelligence.

## Architecture & Data Flow

*(Insert your Architecture Diagram Image Here: `![Architecture Diagram](images/architecture.png)`)*

The pipeline follows a strict **Medallion Architecture** organized within Azure Data Lake Storage Gen2 (ADLS Gen2):
1. **Bronze Zone (Raw):** Landing area for raw CSV and JSON datasets from operational systems.
2. **Silver Zone (Processed):** Data is cleaned, standardized, and validated using PySpark. Duplicates and nulls are handled, and data is converted to highly compressed **Parquet/Delta** formats.
3. **Gold Zone (Curated):** Data is modeled into a Star Schema (Fact and Dimension tables) optimized for read-heavy analytical workloads. 

## Technology Stack
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Compute & Processing:** Azure Synapse Analytics (Spark Pools & Serverless SQL Pools), PySpark
* **Data Format:** Parquet, Delta Lake (ACID transactions & time travel)
* **Modeling:** Dimensional Modeling (Star Schema)
* **Orchestration & Monitoring:** Synapse Pipelines

## Data Model (Star Schema)
The Gold Zone exposes a structured Star Schema to business users via Synapse External Tables/Views.

*(Insert your ERD / Schema Diagram Here: `![ERD Diagram](images/schema_erd.png)`)*

* **`Fact_Sales`**: Core transactional data (Quantity, Price, Discount, Total Amount).
* **`Dim_Product`**: Product details, categories, and historical pricing.
* **`Dim_Customer`**: Customer demographics and segmentation.
* **`Dim_Store`**: Geographic and operational store data.
* **`Dim_Date`**: Standardized time dimensions for time-series analysis (YoY, MoM).

## Key Features & Implementation Details
* **Distributed Transformation:** Leveraged PySpark to handle joins and aggregations efficiently across multiple datasets.
* **Data Quality Framework:** Implemented automated checks during the Bronze-to-Silver transition:
  * Null-value validation for critical primary keys.
  * Schema enforcement and data type standardization.
* **Delta Lake Integration (Advanced):** Utilized Delta Lake format in the Silver/Gold zones to enable ACID transactions, allowing for reliable `UPSERT` operations on customer and product data.
* **Performance Optimization:** Applied **Partitioning** (by Date) and **Z-Ordering** (by Store/Product ID) to drastically reduce query scan times in the Gold layer.
* **Serverless Querying:** Configured Synapse Serverless SQL to allow BI tools to query the Lake directly without provisioning costly dedicated SQL clusters.

## Repository Structure

```text
├── data/
│   ├── sample_raw_data/       # Small samples of CSV/JSON input data
│   └── data_dictionary.md     # Description of fields and data types
├── notebooks/
│   ├── 01_ingest_bronze.ipynb # Raw data ingestion and validation
│   ├── 02_clean_silver.ipynb  # PySpark transformations & Data Quality checks
│   └── 03_model_gold.ipynb    # Star schema creation & Delta optimization
├── sql/
│   ├── create_external_tables.sql # Synapse Serverless SQL DDL
│   └── analytical_queries.sql     # Sample BI queries
├── images/
│   ├── architecture.png
│   ├── schema_erd.png
│   └── pipeline_success.png
└── README.md
