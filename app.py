import textwrap
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

# 1. Page Config (MUST be the very first Streamlit command)
st.set_page_config(page_title="Urban Resilience Dashboard", layout="wide")

# 2. Authentication Logic 
@st.cache_resource
def get_bigquery_client():
    # 1. Try to find Streamlit Cloud Secrets first
    if "gcp_service_account" in st.secrets:
        secret_data = st.secrets["gcp_service_account"]
        
        # Handle both TOML dictionaries and Raw JSON strings securely
        if isinstance(secret_data, str):
            import json
            credentials_dict = json.loads(secret_data)
        else:
            credentials_dict = dict(secret_data)
            
        # --- THE BULLETPROOF PEM RECONSTRUCTOR ---
        pk = credentials_dict.get("private_key", "")
        
        if "-----BEGIN PRIVATE KEY-----" in pk and "-----END PRIVATE KEY-----" in pk:
            # Step A: Extract the raw base64 string without headers
            base64_str = pk.split("-----BEGIN PRIVATE KEY-----")[1].split("-----END PRIVATE KEY-----")[0]
            
            # Step B: Aggressively strip ALL whitespace, spaces, and broken line breaks
            clean_b64 = "".join(base64_str.split())
            
            # Step C: Mathematically chunk it into exactly 64 characters per line (The RFC rule)
            chunked_b64 = "\n".join(textwrap.wrap(clean_b64, 64))
            
            # Step D: Reassemble the perfect cryptographic key
            credentials_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{chunked_b64}\n-----END PRIVATE KEY-----\n"
        # -----------------------------------------
        
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # 2. Fallback to Local Authentication (MacBook)
    else:
        load_dotenv()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gcp_credentials.json"
        return bigquery.Client()
    
# 3. Data Loading Logic
@st.cache_data
def load_data():
    # CALL our custom authentication wrapper!
    client = get_bigquery_client()
    
    query = """
        SELECT * FROM `data-urbanresilience-dev-2335.prod_urban_resilience.gold_air_quality`
        ORDER BY utc_timestamp DESC
    """
    return client.query(query).to_dataframe()


# 4. UI Rendering
st.title("Urban Resilience: Rio de Janeiro Air Quality")
st.markdown("""
This dashboard displays the **Gold Layer** of a Medallion Architecture pipeline. 
The data is extracted from OpenAQ, validated via Pydantic, processed with PySpark, 
and stored in BigQuery using idempotent MERGE logic.
""")

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