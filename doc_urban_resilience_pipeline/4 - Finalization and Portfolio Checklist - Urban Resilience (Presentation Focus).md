
- [ ] **Geospatial Integrity:** Verify that there are no null lat/long coordinates or coordinates outside the range of the chosen region.
- [ ] **Idempotency by Date/Time:** Ensure that when re-processing data from the same hour, BigQuery uses `MERGE` or `OVERWRITE` logic to avoid duplicating measurements.
- [ ] **Schema Validation:** Confirm that dbt or BigQuery is forcing correct data types (e.g., `numeric` for pollutants and `timestamp` for observation).
- [ ] **Urban Impact README:**
    - Display the dbt **Data Lineage** showing the union between sensor and infrastructure data.
    - Include a **Data Dictionary** explaining the extracted pollutant indices.
- [ ] **Soft Skills Demonstration:** In the LinkedIn post, describe how this architecture would help a public manager decide where to install new pollution sensors.