
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CleanEcomData2") \
    .getOrCreate()

try:
    gcs_input_path = "gs://pub-sub-dag-rawdata/*.json"
    print(f"Reading raw JSON data from: {gcs_input_path}")
    
    # 1. Read Raw JSON from GCS Bronze Bucket
    raw_df = spark.read.json(gcs_input_path)
    
    raw_count = raw_df.count()
    print(f"Total raw records read from GCS: {raw_count}")

    if raw_count == 0:
        raise ValueError("CRITICAL ERROR: No records found in GCS input path or JSON files are empty!")

    # Print raw schema to inspect JSON structure in logs
    raw_df.printSchema()

    # 2. Clean, Cast Types, and Deduplicate
    cleaned_df = raw_df.select(
        col("order_id").cast("string"),
        col("product_id").cast("string"),
        col("user_id").cast("string"),
        col("quantity").cast("long"),
        to_timestamp(col("order_date")).alias("order_date")
    ).dropDuplicates(["order_id"])

    cleaned_count = cleaned_df.count()
    print(f"Total cleaned & deduplicated records to write: {cleaned_count}")

    if cleaned_count == 0:
        raise ValueError("CRITICAL ERROR: All records were dropped during cleaning/deduplication!")

    # 3. Save Parquet to Silver GCS Bucket
    parquet_path = "gs://pub-sub-cleandata-dag/parquet/"
    print(f"Writing Parquet data to: {parquet_path}")
    cleaned_df.write.mode("overwrite").parquet(parquet_path)

    # 4. Write directly to Cloud Spanner
    print("Writing data to Cloud Spanner table 'EcomOrders'...")
    cleaned_df.write \
        .format("cloud-spanner") \
        .option("projectId", "snappy-mapper-498509-e0") \
        .option("instanceId", "ecom-spanner-instance") \
        .option("databaseId", "ecom-db") \
        .option("table", "EcomOrders") \
        .mode("append") \
        .save()

    print("Successfully processed and loaded data to Cloud Spanner and GCS.")

except Exception as e:
    print(f"JOB FAILED WITH ERROR: {str(e)}", file=sys.stderr)
    raise e  # Fail the Dataproc batch so Airflow knows the task failed!
finally:
    spark.stop()
