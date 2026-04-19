import os
from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format

# Load environment variables
load_dotenv(find_dotenv())

def process_silver_layer():
    # 1. Initialize the Spark Engine
    print("[1/3] Starting PySpark Session...")
    spark = SparkSession.builder \
        .appName("UrbanResilience_SilverLayer") \
        .getOrCreate()
    
    # Hide overly verbose Spark logs in the terminal
    spark.sparkContext.setLogLevel("WARN")

    bucket_name = os.getenv("GCS_LANDING_BUCKET")
    if not bucket_name:
        raise ValueError("Missing GCS_LANDING_BUCKET in .env")

    # 2. Define the Paths
    # We use the local filesystem for testing before connecting it to GCP Dataproc
    # In a real cloud run, these would be gs:// paths
    source_path = f"gs://{bucket_name}/landing/air_quality/*.json"
    
    # Wait! For our "baby step" today, we will read the JSON from the cloud, 
    # but let's write the Parquet locally to your Mac first to inspect it safely.
    target_local_path = "./data_lake/silver/air_quality/"

    print(f"[2/3] Reading JSON data from GCS Landing Zone...")
    try:
        # PySpark natively reads JSON from GCS using your GOOGLE_APPLICATION_CREDENTIALS
        df = spark.read.json(source_path)
    except Exception as e:
        print(f"Error reading from GCS. Do you have the GCS Connector installed? {e}")
        return

    # 3. Transform: Cast data types and create a Partition Column
    print("[3/3] Converting to Parquet and Partitioning by Date...")
    
    # Convert the string timestamp from OpenAQ into a real Spark Timestamp
    df_transformed = df.withColumn("timestamp_casted", to_timestamp(col("utc_timestamp")))
    
    # Extract just the date (YYYY-MM-DD) to use as a folder partition
    df_partitioned = df_transformed.withColumn("ingest_date", date_format(col("timestamp_casted"), "yyyy-MM-dd"))

    # 4. Write to Parquet
    df_partitioned.write \
        .mode("overwrite") \
        .partitionBy("ingest_date") \
        .parquet(target_local_path)
        
    print(f"Success! Parquet files written locally to: {target_local_path}")
    
    spark.stop()

if __name__ == "__main__":
    process_silver_layer()