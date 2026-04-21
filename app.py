import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import os
import base64
import json
from dotenv import load_dotenv

st.set_page_config(page_title="Urban Resilience Dashboard", layout="wide")

# --- Bulletproof Base64 Authentication ---
@st.cache_resource
def get_bigquery_client():
    # 1. Cloud Execution: Decode the unbreakable Base64 string
    if "GCP_B64" in st.secrets:
        b64_string = st.secrets["GCP_B64"]
        # Decode base64 back to a JSON string, then parse it into a dictionary
        json_string = base64.b64decode(b64_string).decode("utf-8")
        credentials_dict = json.loads(json_string)
        
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # 2. Local MacBook Execution: Use the file
    else:
        load_dotenv()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gcp_credentials.json"
        return bigquery.Client()

# --- Data Loading ---
@st.cache_data
def load_data():
    client = get_bigquery_client()
    query = """
        SELECT * FROM `data-urbanresilience-dev-2335.prod_urban_resilience.gold_air_quality`
        ORDER BY utc_timestamp DESC
    """
    return client.query(query).to_dataframe()

# --- UI Rendering ---
st.title("Urban Resilience: Rio de Janeiro Air Quality")
st.markdown("""
This dashboard displays the **Gold Layer** of a Medallion Architecture pipeline. 
The data is extracted from OpenAQ, validated via Pydantic, processed with PySpark, 
and stored in BigQuery using idempotent MERGE logic.
""")

try:
    df = load_data()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Measurements", len(df))
    col2.metric("Unique Sensors", df['sensor_id'].nunique())
    col3.metric("Avg PM2.5 (Latest)", f"{df['measurement_value'].mean():.2f} µg/m³")

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
    st.error("Failed to connect to BigQuery.")
    st.error(f"System Error: {e}")