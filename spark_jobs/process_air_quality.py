import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format

def process_silver_layer():
    print("[1/3] Starting Cloud PySpark Session...")
    # Notice we removed .master("local[*]") because Dataproc handles the cluster!
    spark = SparkSession.builder \
        .appName("UrbanResilience_SilverLayer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Use your ACTUAL bucket names here
    landing_bucket = "bkt-urbanresil-landing-dev-sa-east1"
    silver_bucket = "bkt-urbanresil-silver-dev-sa-east1"

    # Cloud GCS Paths
    source_path = f"gs://{landing_bucket}/landing/air_quality/*.json"
    target_path = f"gs://{silver_bucket}/air_quality/"

    print(f"[2/3] Reading JSON data directly from GCS...")
    df = spark.read.json(source_path)

    if df.isEmpty():
        print("Dataframe is empty. No files to process.")
        return

    print("[3/3] Converting to Parquet and writing to Silver Bucket...")
    df_transformed = df.withColumn("timestamp_casted", to_timestamp(col("utc_timestamp")))
    df_partitioned = df_transformed.withColumn("ingest_date", date_format(col("timestamp_casted"), "yyyy-MM-dd"))

    # Write natively to the cloud
    df_partitioned.write \
        .mode("overwrite") \
        .partitionBy("ingest_date") \
        .parquet(target_path)
        
    print(f"Success! Parquet files written to: {target_path}")
    spark.stop()

if __name__ == "__main__":
    process_silver_layer()