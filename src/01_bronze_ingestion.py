# 1. Write Bronze Ingestion Script
Set-Content -Path "src\01_bronze_ingestion.py" -Value @'
import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
mlflow.set_experiment("/Shared/banking_poc/01_bronze_layer")

with mlflow.start_run(run_name="bronze_ingestion"):
    volume_base = "/Volumes/retail_banking/bronze/landing"
    
    df_raw_tx = spark.read.json(f"{volume_base}/transactions_json") \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name())
    df_raw_tx.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_banking.bronze.raw_transactions")

    df_raw_cust = spark.read.parquet(f"{volume_base}/customers_parquet") \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name())
    df_raw_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_banking.bronze.raw_customers")

    mlflow.log_metric("bronze_raw_transactions_count", df_raw_tx.count())
    mlflow.log_metric("bronze_raw_customers_count", df_raw_cust.count())
    print("✅ Bronze layer ingestion completed.")
'@