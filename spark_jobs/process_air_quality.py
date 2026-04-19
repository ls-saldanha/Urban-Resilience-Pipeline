import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format

def process_silver_layer():
    print("[1/3] Starting PySpark Session (Local Mode)...")
    spark = SparkSession.builder \
        .appName("UrbanResilience_SilverLayer") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 2. Local Paths (Emulating the Cloud)
    # This reads ANY json file inside our local landing folder
    source_path = "./data_lake/landing/air_quality/*.json"
    target_local_path = "./data_lake/silver/air_quality/"

    print(f"[2/3] Reading JSON data from Local Landing Zone...")
    try:
        df = spark.read.json(source_path)
    except Exception as e:
        print(f"Error reading local data: {e}")
        return

    # 3. Transform: Cast data types and create a Partition Column
    print("[3/3] Converting to Parquet and Partitioning by Date...")
    
    # Check if the dataframe is empty
    if df.isEmpty():
        print("Error: The dataframe is empty. Did you put the JSON file in the right folder?")
        return

    df_transformed = df.withColumn("timestamp_casted", to_timestamp(col("utc_timestamp")))
    df_partitioned = df_transformed.withColumn("ingest_date", date_format(col("timestamp_casted"), "yyyy-MM-dd"))

    # 4. Write to Parquet
    df_partitioned.write \
        .mode("overwrite") \
        .partitionBy("ingest_date") \
        .parquet(target_local_path)
        
    print(f"Success! Parquet files written locally to: {target_local_path}")
    
    # Optional: Let's show the schema so you can see Spark's magic!
    print("\n--- Final Silver Layer Schema ---")
    df_partitioned.printSchema()
    
    spark.stop()

if __name__ == "__main__":
    process_silver_layer()