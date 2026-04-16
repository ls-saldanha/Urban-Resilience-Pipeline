import requests
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
    
    # We use Optional in case the API sometimes misses a timestamp
    date: dict 

# ==========================================
# 2. The Extraction Logic
# ==========================================
def fetch_openaq_data(city: str, limit: int = 10) -> List[dict]:
    """
    Makes an HTTP GET request to the OpenAQ API.
    Returns a list of raw dictionaries.
    """
    url = f"https://api.openaq.org/v2/measurements"
    params = {
        "city": city,
        "limit": limit
    }
    headers = {"Accept": "application/json"}
    
    print(f"Fetching data for {city}...")
    response = requests.get(url, headers=headers, params=params)
    
    # This automatically raises an error if the request fails (e.g., 404 or 500)
    response.raise_for_status() 
    
    # Extract the 'results' array from the JSON response
    return response.json().get('results', [])

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
            # We unpack the dictionary (**) into the Pydantic class
            validated_item = OpenAQMeasurement(**item)
            
            # If it succeeds, we convert it back to a clean dictionary
            valid_data.append(validated_item.model_dump())
        except ValidationError as e:
            print(f"Validation Error! Dropping record. Reason: {e}")
            
    return valid_data

# ==========================================
# 4. Main Execution Block
# ==========================================
if __name__ == "__main__":
    target_city = "Rio de Janeiro"
    
    # Step A: Fetch
    raw_measurements = fetch_openaq_data(city=target_city, limit=5)
    
    # Step B: Validate
    clean_measurements = validate_payload(raw_measurements)
    
    # Step C: Inspect the result (Baby steps!)
    print(f"\nSuccessfully validated {len(clean_measurements)} records.")
    for record in clean_measurements:
        print(f" - {record['location']}: {record['parameter']} = {record['value']} {record['unit']}")