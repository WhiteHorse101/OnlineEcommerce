from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, hour

def clean_data(input_path: str, output_path: str):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RetailDataCleaning") \
        .getOrCreate()

    print("Reading Parquet data...")
    df = spark.read.parquet(input_path)

    print("Initial record count:", df.count())

    # Remove entries with null CustomerID
    df_cleaned = df.filter(col("CustomerID").isNotNull())

    # Remove negative Quantity or UnitPrice (likely refunds or invalid)
    df_cleaned = df_cleaned.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))

    # Convert InvoiceDate to usable datetime format
    df_cleaned = df_cleaned.withColumn("InvoiceYear", year("InvoiceDate")) \
                           .withColumn("InvoiceMonth", month("InvoiceDate")) \
                           .withColumn("InvoiceWeekday", dayofweek("InvoiceDate")) \
                           .withColumn("InvoiceHour", hour("InvoiceDate"))

    print("Cleaned record count:", df_cleaned.count())

    print(f"Writing cleaned data to: {output_path}")
    df_cleaned.write.mode("overwrite").parquet(output_path)

    spark.stop()
    print("Data cleaning complete.")

# Example usage
if __name__ == "__main__":
    input_file = "/Users/rishabhsaudagar/Desktop/NCI CLG/SEMESTER 2/Data Intensive Scalable System/OnlineEcommerce/Data/Raw/online_retail.parquet"
    output_file = "/Users/rishabhsaudagar/Desktop/NCI CLG/SEMESTER 2/Data Intensive Scalable System/OnlineEcommerce/Data/Clean"
    clean_data(input_file, output_file)
