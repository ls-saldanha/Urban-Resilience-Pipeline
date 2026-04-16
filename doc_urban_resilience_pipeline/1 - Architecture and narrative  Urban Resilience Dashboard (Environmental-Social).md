
**Narrative History**
The city hall of a fictional metropolis (**"New Metropolis"**) needs to understand if the density of public transportation in certain neighborhoods helps reduce air pollution. Currently, air quality data and infrastructure data are in different silos. The urban planning department needs a centralized repository to correlate this data and plan new bike lanes and bus lanes.

**Clear Objectives**
* **What?** 
	* Consolidate pollutant measurements (OpenAQ API) with public transport point locations.
* **Why?** 
	* Generate a "Green Mobility Efficiency" index per neighborhood.

**Project Architecture (GCP)**
1.  **Ingestion:** Python script running on **Cloud Functions** (simple and cheap) collects JSON data from the API and saves it to **Google Cloud Storage (GCS)** in the `landing` folder.
2.  **Processing:** A **PySpark** job cleans nested JSONs and converts geographic coordinates into neighborhood names, saving in **Parquet** format in the `processed` folder.
3.  **Data Warehouse:** Loading data into **BigQuery**.
4.  **Analytical Transformation:** Use of **dbt** (or SQL in BQ) to create the Gold layer, joining pollutants and transportation into a final table ready for BI.