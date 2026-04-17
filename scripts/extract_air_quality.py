import datetime
from pydantic import BaseModel, ValidationError
from typing import List, Optional

# ==========================================
# 1. The Data Contract (Pydantic Models)
# ==========================================
class OpenAQMeasurement(BaseModel):
    locationId: int
    location: str
    parameter: str
    value: float
    unit: str
    date: dict 

# ==========================================
# 2. The Extraction Logic (MOCKED FOR V3)
# ==========================================
def fetch_openaq_data(city: str, limit: int = 10) -> List[dict]:
    """
    MOCK FUNCTION: Simulates fetching data from OpenAQ v3.
    TODO: Replace with actual requests.get() once OpenAQ API Key is registered.
    """
    print(f"Fetching [MOCKED] data for {city}...")
    
    # We use the new, timezone-aware method introduced in recent Python versions
    current_time = datetime.datetime.now(datetime.UTC).isoformat()
    
    
    mock_results = [
        {
            "locationId": 101,
            "location": "São Paulo - Ibirapuera",
            "parameter": "pm25",
            "value": 15.2,
            "unit": "µg/m³",
            "date": {"utc": current_time, "local": current_time}
        },
        {
            "locationId": 102,
            "location": "São Paulo - Pinheiros",
            "parameter": "no2",
            "value": 32.0,
            "unit": "ppm",
            "date": {"utc": current_time, "local": current_time}
        },
        {   
            # Let's inject a "bad" record to prove our Pydantic bouncer works!
            "locationId": 103,
            "location": "São Paulo - Centro",
            "parameter": "o3",
            "value": "ERROR_SENSOR_OFFLINE", # Pydantic will catch this string!
            "unit": "ppm",
            "date": {"utc": current_time, "local": current_time}
        }
    ]
    
    return mock_results[:limit]

# ==========================================
# 3. The Validation Logic
# ==========================================
def validate_payload(raw_data: List[dict]) -> List[dict]:
    """
    Passes the raw JSON dictionaries through our Pydantic model.
    """
    valid_data = []
    for item in raw_data:
        try:
            validated_item = OpenAQMeasurement(**item)
            valid_data.append(validated_item.model_dump())
        except ValidationError as e:
            # It will catch the "ERROR_SENSOR_OFFLINE" string!
            print(f"\n[!] Validation Error! Dropping record for {item.get('location')}.")
            print(f"    Reason: The 'value' field must be a valid number.\n")
            
    return valid_data

# ==========================================
# 4. Main Execution Block
# ==========================================
if __name__ == "__main__":
    target_city = "São Paulo"
    
    # Step A: Fetch (Mocked)
    raw_measurements = fetch_openaq_data(city=target_city, limit=5)
    
    # Step B: Validate
    clean_measurements = validate_payload(raw_measurements)
    
    # Step C: Inspect the result
    print(f"\nSuccessfully validated {len(clean_measurements)} records.")
    for record in clean_measurements:
        print(f" - {record['location']}: {record['parameter']} = {record['value']} {record['unit']}")