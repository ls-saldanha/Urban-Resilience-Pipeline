import streamlit as st
from google.cloud import bigquery
import os
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv

# Load credentials
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gcp_credentials.json"

st.set_page_config(page_title="Urban Resilience Dashboard", layout="wide")

st.title("Urban Resilience: Rio de Janeiro Air Quality")
st.markdown("""
This dashboard displays the **Gold Layer** of a Medallion Architecture pipeline. 
The data is extracted from OpenAQ, validated via Pydantic, processed with PySpark, 
and stored in BigQuery using idempotent MERGE logic.
""")

@st.cache_data
def load_data():
    client = bigquery.Client()
    # Replace with your actual project ID if different
    query = """
        SELECT * FROM `data-urbanresilience-dev-2335.prod_urban_resilience.gold_air_quality`
        ORDER BY utc_timestamp DESC
    """
    return client.query(query).to_dataframe()

try:
    df = load_data()

    # --- KPI Row ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Measurements", len(df))
    col2.metric("Unique Sensors", df['sensor_id'].nunique())
    col3.metric("Avg PM2.5 (Latest)", f"{df['measurement_value'].mean():.2f} µg/m³")

    # --- Map & Chart ---
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Sensor Locations (Rio de Janeiro)")
        fig_map = px.scatter_mapbox(df, lat="latitude", lon="longitude", 
            color="measurement_value", size="measurement_value",
            color_continuous_scale=px.colors.cyclical.IceFire, 
            size_max=15, zoom=10, mapbox_style="carto-positron")
        st.plotly_chart(fig_map, use_container_width=True)

    with right_col:
        st.subheader("Pollution Levels Over Time")
        fig_line = px.line(df, x="utc_timestamp", y="measurement_value", color="sensor_id")
        st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("Raw Gold Data (Deduplicated)")
    st.dataframe(df.head(100), use_container_width=True)

except Exception as e:
    st.error(f"Failed to connect to BigQuery: {e}")
    st.info("Ensure your gcp_credentials.json is correct and you have run the MERGE script.")