import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import random

spark = SparkSession.builder.getOrCreate()

# MLflow Experiment Tracking
mlflow.set_experiment("/Shared/banking_poc/00_data_generation")

with mlflow.start_run(run_name="pyspark_data_generator"):
    volume_base = "/Volumes/retail_banking/bronze/landing"
    json_path = f"{volume_base}/transactions_json"
    parquet_path = f"{volume_base}/customers_parquet"

    # 1. Generate Customer Master (Parquet) - 500 Customers
    num_customers = 500
    first_names = ["Adam", "Siti", "Chen", "Raj", "Sarah", "Ismail", "Mei", "David", "Aisha", "Ken"]
    last_names = ["Tan", "Ahmad", "Wong", "Kumar", "Smith", "Hassan", "Lee", "Jones", "Ali", "Chua"]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    tier_weights = [0.4, 0.35, 0.2, 0.05]
    countries = ["MY", "SG", "TH", "ID"]

    cust_data = []
    for i in range(1, num_customers + 1):
        c_id = f"CUST{i:05d}"
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        tier = random.choices(tiers, weights=tier_weights)[0]
        country = random.choice(countries)
        reg_date = f"{random.randint(2023, 2025)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        cust_data.append((c_id, name, tier, country, reg_date))

    cust_schema = StructType([
        StructField("cust_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registered_date", StringType(), True)
    ])

    df_customers = spark.createDataFrame(cust_data, schema=cust_schema)
    df_customers.write.mode("overwrite").parquet(parquet_path)

    mlflow.log_param("num_customers", num_customers)
    mlflow.log_param("parquet_output_path", parquet_path)
    mlflow.log_metric("customers_generated_count", df_customers.count())

    # 2. Generate 3 Years of Transactions (JSON) - 2024 to 2026
    start_date = "2024-01-01 00:00:00"
    end_date = "2026-10-15 23:59:59"

    df_dates = spark.range(1).select(
        F.explode(
            F.sequence(
                F.to_timestamp(F.lit(start_date)),
                F.to_timestamp(F.lit(end_date)),
                F.expr("interval 1 hour")
            )
        ).alias("base_time")
    )

    df_tx = df_dates.select(
        F.concat(F.lit("TX"), F.date_format("base_time", "yyyyMMddHH"), F.expr("CAST(RAND() * 1000 AS INT)")).alias("tx_id"),
        F.concat(F.lit("CUST"), F.lpad(F.expr("CAST(FLOOR(RAND() * 500) + 1 AS INT)"), 5, "0")).alias("cust_id"),
        F.round(
            F.expr("""
                CASE 
                    WHEN RAND() < 0.60 THEN RAND() * 80 + 5
                    WHEN RAND() < 0.90 THEN RAND() * 400 + 100
                    ELSE RAND() * 2500 + 500
                END
            """), 2
        ).alias("amount"),
        F.expr("""
            CASE 
                WHEN RAND() < 0.45 THEN 'MOBILE'
                WHEN RAND() < 0.75 THEN 'POS'
                WHEN RAND() < 0.90 THEN 'WEB'
                ELSE 'ATM'
            END
        """).alias("channel"),
        F.date_format("base_time", "yyyy-MM-dd HH:mm:ss").alias("timestamp")
    )

    df_tx.write.mode("overwrite").json(json_path)

    tx_count = df_tx.count()
    mlflow.log_param("json_output_path", json_path)
    mlflow.log_metric("transactions_generated_count", tx_count)

    print(f"Data Generation Complete: {num_customers} customers, {tx_count} transactions generated.")