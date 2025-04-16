from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, countDistinct, sum as spark_sum, to_date, concat_ws
)

def transform_data(input_path: str, output_path: str):
    spark = SparkSession.builder \
        .appName("RetailDataTransformation") \
        .getOrCreate()

    print("Reading cleaned data...")
    df = spark.read.parquet(input_path)

    # Add TotalPrice column
    df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

    # Invoice-level purchase value per customer
    invoice_summary = df.groupBy("InvoiceNo", "CustomerID") \
                        .agg(
                            spark_sum("TotalPrice").alias("InvoiceTotal"),
                            countDistinct("StockCode").alias("ItemCount")
                        )

    # Add Date-only version of InvoiceDate (remove time)
    df = df.withColumn("InvoiceDateOnly", to_date("InvoiceDate"))

    # Prepare transaction format for market basket (InvoiceNo as basket)
    basket_df = df.groupBy("InvoiceNo") \
                  .agg(concat_ws(",", expr("collect_set(Description)")).alias("ItemList"))

    print("Saving transformed data...")
    df.write.mode("overwrite").parquet(output_path + "/transaction_data")
    invoice_summary.write.mode("overwrite").parquet(output_path + "/invoice_summary")
    basket_df.write.mode("overwrite").parquet(output_path + "/basket_data")

    spark.stop()
    print("Data transformation complete.")

# Example usage
if __name__ == "__main__":
    input_file = "/Users/rishabhsaudagar/Desktop/NCI CLG/SEMESTER 2/Data Intensive Scalable System/OnlineEcommerce/Data/Clean/part-00000-562accda-9ef1-40d6-8542-6d591cb00821-c000.snappy.parquet"  # or Azure Blob wasbs:// path
    output_dir = "OnlineEcommerce/Data/Transformed"
    transform_data(input_file, output_dir)
