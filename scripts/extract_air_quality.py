import os
import requests
import json
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, ValidationError
from typing import List, Optional

load_dotenv(find_dotenv())

# ==========================================
# 1. The Data Contract (Source-Aligned)
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
# 2. The Extraction Logic
# ==========================================
def get_rio_locations(api_key: str) -> List[int]:
    """Finds all OpenAQ location IDs within 25km of central Rio."""
    url = "https://api.openaq.org/v3/locations"
    params = {
        "coordinates": "-22.9068,-43.1729",
        "radius": 25000, 
        "limit": 5 
    }
    headers = {"X-API-Key": api_key}
    
    print("\n[1/3] Using radar to find sensors in Rio de Janeiro...")
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    locations = response.json().get('results', [])
    location_ids = [loc['id'] for loc in locations]
    print(f"      -> Found {len(location_ids)} active stations: {location_ids}")
    return location_ids

def fetch_latest_measurements(location_id: int, api_key: str) -> List[dict]:
    """Fetches the latest air quality readings for a specific station ID."""
    url = f"https://api.openaq.org/v3/locations/{location_id}/latest"
    headers = {"X-API-Key": api_key}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('results', [])

# ==========================================
# 3. The Validation Logic
# ==========================================
def validate_payload(raw_data: List[dict]) -> List[dict]:
    valid_data = []
    for item in raw_data:
        try:
            validated = OpenAQMeasurement(**item)
            
            # We flatten the JSON so it's ready for Parquet/BigQuery later
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
        except ValidationError as e:
            print(f"\n[!] Bouncer rejected a record! Error: {e}")
            
    return valid_data

# ==========================================
# 4. Main Execution Block
# ==========================================
if __name__ == "__main__":
    API_KEY = os.getenv("OPENAQ_API_KEY")
    if not API_KEY:
        print("ERROR: Missing API Key!")
        exit(1)
        
    rio_station_ids = get_rio_locations(API_KEY)
    
    print("\n[2/3] Fetching and validating data for each station...")
    all_clean_measurements = []
    
    for station_id in rio_station_ids:
        raw_data = fetch_latest_measurements(station_id, API_KEY)
        clean_data = validate_payload(raw_data)
        all_clean_measurements.extend(clean_data)
        
    print(f"\n[3/3] Success! Validated {len(all_clean_measurements)} total records.")
    
    # Let's look at the first 3 perfectly clean, flattened records
    print(json.dumps(all_clean_measurements[:3], indent=4))