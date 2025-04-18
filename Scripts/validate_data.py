from pyspark.sql import SparkSession
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

base = "wasbs://retail-data@dataintensiveproject.blob.core.windows.net/transformed"

for name in ["invoice_summary", "basket_data", "customer_rfm", "time_agg"]:
    path = f"{base}/{name}"
    df = spark.read.parquet(path)
    print(f"\n=== SAMPLE from {name} ===")
    df.printSchema()
    df.show(5, truncate=False)

spark.stop()
