#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct, collect_set,
    to_date, datediff, max as spark_max, lit
)
from dotenv import load_dotenv
import os

# Load Azure credentials from .env
load_dotenv()
account_name = os.getenv("AZURE_STORAGE_ACCOUNT", "dataintensiveproject")
account_key = os.getenv("AZURE_STORAGE_KEY")
if not account_key:
    raise ValueError("AZURE_STORAGE_KEY environment variable not set")

# Initialize Spark with Azure Blob connector
spark = SparkSession.builder \
    .appName("RetailDataTransformation") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6"
    ) \
    .config(
        f"fs.azure.account.key.{account_name}.blob.core.windows.net",
        account_key
    ) \
    .getOrCreate()

# Define paths
input_path   = f"wasbs://retail-data@{account_name}.blob.core.windows.net/clean/retail_cleaned"
output_base  = f"wasbs://retail-data@{account_name}.blob.core.windows.net/transformed"

# Read cleaned data
df = spark.read.parquet(input_path)

# 1. Invoice‐level summary: total spend and distinct item count per invoice
invoice_summary = (
    df.groupBy("InvoiceNo", "CustomerID")
      .agg(
          spark_sum(col("Quantity") * col("UnitPrice")).alias("InvoiceTotal"),
          countDistinct("StockCode").alias("ItemCount")
      )
)
invoice_summary.write.mode("overwrite").parquet(f"{output_base}/invoice_summary")

# 2. Basket data for market‐basket analysis: list of items per invoice
basket_data = (
    df.groupBy("InvoiceNo")
      .agg(collect_set("Description").alias("ItemList"))
)
basket_data.write.mode("overwrite").parquet(f"{output_base}/basket_data")

# 3. RFM features per customer
#    - Reference date = max InvoiceDate in the dataset
max_date = df.select(spark_max(col("InvoiceDate")).alias("refDate")).collect()[0]["refDate"]

rfm = (
    df.groupBy("CustomerID")
      .agg(
          spark_max(col("InvoiceDate")).alias("LastPurchase"),
          countDistinct("InvoiceNo").alias("Frequency"),
          spark_sum(col("Quantity") * col("UnitPrice")).alias("Monetary")
      )
      .withColumn("Recency", datediff(lit(max_date), col("LastPurchase")))
)
rfm.write.mode("overwrite").parquet(f"{output_base}/customer_rfm")

# 4. Time‐based sales aggregation (daily totals)
daily_sales = (
    df.withColumn("Date", to_date(col("InvoiceDate")))
      .groupBy("Date")
      .agg(spark_sum(col("Quantity") * col("UnitPrice")).alias("DailySales"))
)
daily_sales.write.mode("overwrite").parquet(f"{output_base}/time_agg")

spark.stop()
print("Data transformation complete and datasets written to:")
print(f"  • {output_base}/invoice_summary")
print(f"  • {output_base}/basket_data")
print(f"  • {output_base}/customer_rfm")
print(f"  • {output_base}/time_agg")
