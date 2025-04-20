from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, hour
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Set Azure Storage details
account_name = "dataintensiveproject"
account_key = os.getenv("AZURE_STORAGE_KEY")
if not account_key:
    raise ValueError("AZURE_STORAGE_KEY environment variable not set.")

# Initialize Spark session with Azure Blob Storage configurations
spark = SparkSession.builder \
    .appName("RetailDataCleaning") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key) \
    .getOrCreate()

# Define input and output paths using wasbs:// protocol
input_path = f"wasbs://retail-data@{account_name}.blob.core.windows.net/raw/online_retail.parquet"
output_path = f"wasbs://retail-data@{account_name}.blob.core.windows.net/clean/retail_cleaned"

print("Reading raw data from Azure Blob Storage...")
df = spark.read.parquet(input_path)

# Data cleaning: Remove records with null CustomerID and negative or zero values; add time features
df_cleaned = df.filter(col("CustomerID").isNotNull()) \
               .filter((col("Quantity") > 0) & (col("UnitPrice") > 0)) \
               .withColumn("InvoiceYear", year("InvoiceDate")) \
               .withColumn("InvoiceMonth", month("InvoiceDate")) \
               .withColumn("InvoiceWeekday", dayofweek("InvoiceDate")) \
               .withColumn("InvoiceHour", hour("InvoiceDate"))

print("Writing cleaned data to Azure Blob Storage...")
# Write cleaned data; Spark will create a folder structure with partition files and a _SUCCESS marker
df_cleaned.write.mode("overwrite").parquet(output_path)

spark.stop()
print(f"Data cleaning complete and output written to: {output_path}")
