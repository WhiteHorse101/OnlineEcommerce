#!/usr/bin/env python

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

# 1. Load environment variables
load_dotenv()
account_name = os.getenv("AZURE_STORAGE_ACCOUNT", "dataintensiveproject")
account_key  = os.getenv("AZURE_STORAGE_KEY")
sql_url      = os.getenv("AZURE_SQL_URL")       # e.g. jdbc:sqlserver://retailsqlsrv.database.windows.net:1433;database=retail_analytics_database;...
sql_user     = os.getenv("AZURE_SQL_USER")      # e.g. retailadmin@retailsqlsrv
sql_pass     = os.getenv("AZURE_SQL_PASSWORD")  # your SQL admin password

if not all([account_key, sql_url, sql_user, sql_pass]):
    raise ValueError("Missing one or more required environment variables")

print("🛠️  Starting DB Loader script")

# 2. Initialize SparkSession with Azure Blob and JDBC driver packages
spark = SparkSession.builder \
    .appName("DBLoader") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-azure:3.3.1",
            "com.microsoft.azure:azure-storage:8.6.6",
            "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8"
        ])
    ) \
    .config(
        f"fs.azure.account.key.{account_name}.blob.core.windows.net",
        account_key
    ) \
    .getOrCreate()

print("SparkSession created")

base_path = f"wasbs://retail-data@{account_name}.blob.core.windows.net/transformed"

def load_to_sql(folder: str, table_name: str):
    print(f"▶️  Loading folder '{folder}' into table '{table_name}'")
    df = spark.read.parquet(f"{base_path}/{folder}")

    # Flatten array<string> for basket_data
    if folder == "basket_data":
        df = df.withColumn("ItemList", concat_ws(",", col("ItemList")))

    df.write \
      .format("jdbc") \
      .mode("overwrite") \
      .option("url",     sql_url) \
      .option("dbtable", table_name) \
      .option("user",    sql_user) \
      .option("password", sql_pass) \
      .option("driver",  "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
      .save()

    print(f"Finished loading '{table_name}'")

# 3. Load each transformed dataset
load_to_sql("invoice_summary", "InvoiceSummary")
load_to_sql("basket_data",     "BasketData")
load_to_sql("customer_rfm",     "CustomerRFM")
load_to_sql("time_agg",         "DailySales")

spark.stop()
print("All tables successfully loaded into Azure SQL.")
