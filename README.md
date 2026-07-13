# Databricks-formula1-project

**Project Architecture & Tech Stack**
This project leverages the Medallion Architecture (Bronze → Silver → Gold) to systematically improve data quality and structure.
**Data Source**: Historical and incremental F1 data (1950 to present) typically sourced from the Ergast Motor Racing Holding API. Files include circuits, races, constructors, drivers, results, pit_stops, lap_times, and qualifying.
**Storage:** Azure Data Lake Storage (ADLS) Gen2 split into raw, ingested, and presentation containers.
**Processing Engine:** Azure Databricks executing PySpark and Spark SQL code over scalable clusters.
**Orchestration**: Azure Data Factory (ADF) to schedule, run, and monitor notebooks sequentially.
**Data Governance:** Unity Catalog for centralized access control and audit trails.
**Security:** Azure Key Vault for managing storage account access keys and secrets securely.

**Core Pipeline Workflow**

[Ergast API / Raw Files] 
       │
       ▼ (Azure Data Factory Orchestration)
[ADLS Gen2 Raw Container] 
       │
       ▼ (Bronze Layer: Schema Enforcement & Delta format)
[ADLS Gen2 Ingested Container] 
       │
       ▼ (Silver Layer: Cleaning, Renaming & Joins)
[ADLS Gen2 Presentation Container] 
       │
       ▼ (Gold Layer: Aggregations & Analytics)
[Databricks Dashboards / Power BI]

**1. Setup & Connection** Create required resources in the Azure Portal.Mount ADLS Gen2 containers to the Databricks Workspace using Service Principals and Azure Key Vault.
**2. Data Ingestion** (Bronze Layer)Read multi-format raw data files (CSV, single-line JSON, and multi-line JSON) using PySpark DataFrames.Apply rigid data schemas and insert standard audit columns like ingestion_date and file_source.Save the ingested data into Delta Lake format to achieve transactional capability.Configure the ingestion logic to handle incremental updates (upserts) for weekly race additions using Delta MERGE commands.
**3. Data Transformation** (Silver Layer)Cleanse data by dropping unnecessary columns and renaming others to standardized naming conventions.Join dimension tables (drivers, constructors, races, circuits) with the factual results dataset.Filter out invalid data records and write the output back into the transformation layer.
**4. Analytics & Presentation** (Gold Layer)Aggregate race metrics using PySpark Window functions and Spark SQL.Calculate real-time season metrics: Driver Standings and Constructor Standings.Extract long-term analytics such as "Most Dominant Drivers" or "Top Pit Crew Teams" across different eras.
**5. Reporting & Visualizations** Build native Databricks Dashboards directly inside notebooks using SQL visualizations.Connect the Gold Delta tables directly to external BI tools like Microsoft Power BI for executive reporting.

