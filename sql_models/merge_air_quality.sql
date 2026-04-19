-- Step 1: Create the production table if it doesn't exist yet
CREATE TABLE IF NOT EXISTS `data-urbanresilience-dev-2335.prod_urban_resilience.gold_air_quality` (
    location_id INT64,
    sensor_id INT64,
    measurement_value FLOAT64,
    utc_timestamp TIMESTAMP,
    latitude FLOAT64,
    longitude FLOAT64,
    ingest_date DATE,
    last_updated TIMESTAMP
)
PARTITION BY ingest_date;

-- Step 2: The MERGE Command (Idempotent Upsert)
MERGE `data-urbanresilience-dev-2335.prod_urban_resilience.gold_air_quality` AS target
USING `data-urbanresilience-dev-2335.stg_urban_resilience.ext_air_quality` AS source
ON 
    target.location_id = source.location_id AND
    target.sensor_id = source.sensor_id AND 
    target.utc_timestamp = CAST(source.utc_timestamp AS TIMESTAMP)
WHEN NOT MATCHED THEN
    -- If the exact sensor reading at that exact second doesn't exist, insert it!
    INSERT (
        location_id, 
        sensor_id, 
        measurement_value, 
        utc_timestamp, 
        latitude, 
        longitude, 
        ingest_date,
        last_updated
    )
    VALUES (
        source.location_id, 
        source.sensor_id, 
        source.measurement_value, 
        CAST(source.utc_timestamp AS TIMESTAMP), 
        source.latitude, 
        source.longitude, 
        CAST(source.ingest_date AS DATE),
        CURRENT_TIMESTAMP()
    );

    