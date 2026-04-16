
- [ ] **Step 1 — Setup:** Create a project on GCP and a Service Account with Storage and BigQuery permissions.
- [ ] **Step 2 — API Collection:** Create the `extract_air_quality.py` script to fetch data from OpenAQ.
- [ ] **Step 3 — Validation:** Use Pydantic to ensure the API JSON contains latitude, longitude, and pollutant value.
- [ ] **Step 4 — GCS Landing:** Send raw JSON to `gs://bucket/landing/raw_air_data/`.
- [ ] **Step 5 — Spark Processing:** PySpark job to "flatten" the JSON and create the structure: `date, location, pollutant, value, unit`.
- [ ] **Step 6 — BQ Staging:** Create an external table in BigQuery pointing to the Parquet files.
- [ ] **Step 7 — Stored Procedure/dbt:** Create SQL logic that removes old data from the same date and inserts new data (Merge/Upsert).
- [ ] **Step 8 — Documentation:** Explain why the correlation between pollution and transportation is valuable for resilient cities.