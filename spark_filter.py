import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Tworzymy parser argumentów
parser = argparse.ArgumentParser(description="Script to filter sales data")

parser.add_argument("--input_path", required=True, type=str, help="Path to the input CSV file")
parser.add_argument("--output_path", required=True, type=str, help="Path to save the filtered Parquet file")
parser.add_argument("--min_sales", required=True, type=int, help="Minimum sales threshold for filtering")

args = parser.parse_args()

spark = SparkSession.builder.appName("SalesFilter").getOrCreate()

df = spark.read.option("header", "true").csv(args.input_path)

df = df.withColumn("quantity_sold", col("quantity_sold").cast("int"))

df_filtered = df.filter(col("quantity_sold") >= args.min_sales)

df_filtered.write.mode("overwrite").parquet(args.output_path)

df_filtered.show(10)
