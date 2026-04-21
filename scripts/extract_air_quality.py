import os
import requests
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, ValidationError
from typing import List, Optional
from google.cloud import storage # <-- NEW IMPORT

load_dotenv(find_dotenv())

# ==========================================
# 1. The Data Contract
# ==========================================
class DatetimeDef(BaseModel):
    utc: str
    local: str

class CoordinatesDef(BaseModel):
    latitude: float
    longitude: float

class OpenAQMeasurement(BaseModel):
    datetime: DatetimeDef
    value: float
    coordinates: Optional[CoordinatesDef] = None
    sensorsId: int
    locationsId: int

# ==========================================
# 2. API Extraction Logic
# ==========================================
def get_rio_locations(api_key: str) -> List[int]:
    url = "https://api.openaq.org/v3/locations"
    params = {"coordinates": "-22.9068,-43.1729", "radius": 25000, "limit": 5}
    headers = {"X-API-Key": api_key}
    
    print("\n[1/4] Using radar to find sensors in Rio de Janeiro...")
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    locations = response.json().get('results', [])
    return [loc['id'] for loc in locations]

def fetch_latest_measurements(location_id: int, api_key: str) -> List[dict]:
    url = f"https://api.openaq.org/v3/locations/{location_id}/latest"
    headers = {"X-API-Key": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('results', [])

def fetch_measurements(location_id: int):
    """Fetches air quality data for a specific location for the last 7 days."""
    
    # 2. Calculate the "7 days ago" timestamp right before the request
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    url = f"https://api.openaq.org/v3/locations/{location_id}/measurements"
    
    # 3. Add 'date_from' to your parameters
    params = {
        "limit": 1000,
        "date_from": seven_days_ago  # <--- This tells the API: "Give me everything since April 14"
    }
    
    headers = {"X-API-Key": os.getenv("OPENAQ_API_KEY")}
    
    response = requests.get(url, headers=headers, params=params)


# ==========================================
# 3. Validation Logic
# ==========================================
def validate_payload(raw_data: List[dict]) -> List[dict]:
    valid_data = []
    for item in raw_data:
        try:
            validated = OpenAQMeasurement(**item)
            clean_record = {
                "location_id": validated.locationsId,
                "sensor_id": validated.sensorsId,
                "measurement_value": validated.value,
                "utc_timestamp": validated.datetime.utc
            }
            if validated.coordinates:
                clean_record["latitude"] = validated.coordinates.latitude
                clean_record["longitude"] = validated.coordinates.longitude
            valid_data.append(clean_record)
        except ValidationError:
            pass 
    return valid_data

# ==========================================
# 4. Storage Logic (NEW!)
# ==========================================
def upload_to_gcs(bucket_name: str, data: List[dict]):
    """Streams data directly from memory into a GCS bucket."""
    print(f"\n[3/4] Preparing {len(data)} records for Google Cloud Storage...")
    
    # Generate a dynamic filename based on the current time
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    destination_blob_name = f"landing/air_quality/rio_measurements_{timestamp}.json"
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    print(f"[4/4] Uploading to gs://{bucket_name}/{destination_blob_name}...")
    
    # upload_from_string handles the in-memory stream!
    blob.upload_from_string(
        data=json.dumps(data),
        content_type='application/json'
    )
    print("      -> Success! Data safely landed in the Cloud.")

# ==========================================
# 5. Main Execution Block
# ==========================================
if __name__ == "__main__":
    API_KEY = os.getenv("OPENAQ_API_KEY")
    GCS_BUCKET = os.getenv("GCS_LANDING_BUCKET")
    
    if not API_KEY or not GCS_BUCKET:
        print("ERROR: Missing API Key or GCS Bucket in .env file!")
        exit(1)
        
    rio_station_ids = get_rio_locations(API_KEY)
    
    print("\n[2/4] Fetching and validating data for each station...")
    all_clean_measurements = []
    for station_id in rio_station_ids:
        raw_data = fetch_latest_measurements(station_id, API_KEY)
        clean_data = validate_payload(raw_data)
        all_clean_measurements.extend(clean_data)
        
    if all_clean_measurements:
        upload_to_gcs(GCS_BUCKET, all_clean_measurements)
    else:
        print("No valid data found to upload.")