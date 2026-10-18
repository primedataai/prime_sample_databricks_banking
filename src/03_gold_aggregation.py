# 3. Write Gold Aggregation Script
Set-Content -Path "src\03_gold_aggregation.py" -Value @'
import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
mlflow.set_experiment("/Shared/banking_poc/03_gold_layer")

with mlflow.start_run(run_name="gold_aggregation"):
    fact_tx = spark.table("retail_banking.silver.fact_transactions")
    dim_cust = spark.table("retail_banking.silver.dim_customers")

    df_gold = fact_tx.join(dim_cust, on="cust_id", how="inner") \
        .withColumn("transaction_date", F.to_date("timestamp")) \
        .groupBy("transaction_date", "cust_id", "customer_name", "tier", "country", "channel") \
        .agg(
            F.round(F.sum("amount"), 2).alias("total_spend"),
            F.round(F.avg("amount"), 2).alias("avg_tx_amount"),
            F.count("tx_id").alias("transaction_count")
        )

    gold_table = "retail_banking.gold.agg_daily_customer_spend"
    df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)

    summary = df_gold.select(F.sum("total_spend").alias("total_spend")).collect()[0]
    mlflow.log_metric("gold_total_spend", float(summary["total_spend"]))
    print(f"✅ Gold aggregation completed. 3-Year Spend: ${summary['total_spend']:,.2f}")
'@