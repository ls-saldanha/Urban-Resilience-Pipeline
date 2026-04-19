# Urban Resilience Pipeline: A Medallion Architecture for Rio de Janeiro Air Quality

## Project Overview
This project is an automated, end-to-end Data Engineering pipeline designed to ingest, validate, and process environmental data (Air Quality) for the city of Rio de Janeiro. By implementing a strict Medallion Architecture (Bronze, Silver, Gold), the pipeline provides reliable, idempotent, and highly available data for urban planners and data scientists to correlate pollution metrics with public transport utilization.

## Business Value
Urban resilience depends on accurate, historical environmental data. Traditional ETL pipelines often suffer from data duplication, silent schema drift, and high compute costs. This pipeline solves these issues by:
* Enforcing strict data contracts at the ingestion layer to prevent bad data from entering the Data Lake.
* Decoupling storage from compute using Cloud Object Storage and External Tables, drastically reducing warehouse costs.
* Utilizing idempotent UPSERT logic to guarantee data integrity, regardless of how many times the pipeline is executed.

## Architecture & Data Flow

The pipeline embraces the modern ELT (Extract, Load, Transform) paradigm:

1. **Source:** Geospatial API extraction from OpenAQ (v3) targeting a 25km radius around central Rio de Janeiro.
2. **Extract & Validate (Python):** Python scripts intercept the raw JSON payload. Pydantic is used to enforce strict schema contracts, perform type coercion, and flatten nested structures.
3. **Bronze Layer (Google Cloud Storage):** Validated, flattened JSON is streamed directly from memory into a GCS Landing Bucket.
4. **Silver Layer (PySpark):** A Spark job reads the Landing Zone data, casts data types, and writes the output back to GCS as highly compressed, column-oriented Parquet files, utilizing Hive-style partitioning by date.
5. **Staging (BigQuery External Tables):** BigQuery acts as a compute engine layered over the GCS Data Lakehouse, reading the partitioned Parquet files dynamically without duplicating storage costs.
6. **Gold Layer (BigQuery MERGE):** A SQL model utilizes the `MERGE` command to perform an idempotent upsert from the Staging table into the Production table, ensuring zero duplicate records.

## Key Technologies
* **Language & Environment:** Python 3.13, managed by `uv` for deterministic dependency resolution.
* **Data Contracts:** Pydantic (Recursive descent parsing and validation).
* **Distributed Processing:** PySpark (Format conversion and partitioning).
* **Cloud Infrastructure:** Google Cloud Platform (GCS, BigQuery, IAM Service Accounts).
* **Data Formats:** JSON (Ingestion), Parquet with Snappy compression (Storage).

## Engineering Highlights

### Shift-Left Data Quality
Rather than cleaning data in the warehouse, data quality is pushed to the very edge of the pipeline. Pydantic models act as a "bouncer," silently dropping API payloads that violate the structural contract, ensuring the Data Lake is immune to upstream schema drift.

### In-Memory Serverless Streaming
The ingestion script bypasses local disk I/O entirely. Data is transformed into JSON strings in RAM and streamed directly to Google Cloud Storage. This pattern ensures the script can be deployed to ephemeral serverless containers (like Google Cloud Run) without crashing due to disk space limitations.

### Idempotent Production Models
The final Gold layer relies on BigQuery `MERGE` statements rather than basic `INSERT` or expensive `TRUNCATE/RELOAD` patterns. By comparing sensor IDs and exact timestamps, the pipeline safely ignores duplicate executions, making the architecture highly resilient to scheduling errors or manual backfills.