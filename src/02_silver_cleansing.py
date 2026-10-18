# 2. Write Silver Cleansing Script
Set-Content -Path "src\02_silver_cleansing.py" -Value @'
import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
mlflow.set_experiment("/Shared/banking_poc/02_silver_layer")

with mlflow.start_run(run_name="silver_cleansing"):
    df_tx = spark.table("retail_banking.bronze.raw_transactions")
    df_cust = spark.table("retail_banking.bronze.raw_customers")

    df_silver_tx = df_tx.filter(F.col("tx_id").isNotNull() & (F.col("amount") > 0)) \
        .dropDuplicates(["tx_id"]) \
        .withColumn("timestamp", F.to_timestamp("timestamp")) \
        .withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
        .select("tx_id", "cust_id", "amount", "channel", "timestamp")
    df_silver_tx.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_banking.silver.fact_transactions")

    df_silver_cust = df_cust.filter(F.col("cust_id").isNotNull()) \
        .dropDuplicates(["cust_id"]) \
        .withColumn("registered_date", F.to_date("registered_date")) \
        .select("cust_id", "customer_name", "tier", "country", "registered_date")
    df_silver_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_banking.silver.dim_customers")

    mlflow.log_metric("silver_fact_transactions_count", df_silver_tx.count())
    mlflow.log_metric("silver_dim_customers_count", df_silver_cust.count())
    print("✅ Silver layer cleansing completed.")
'@