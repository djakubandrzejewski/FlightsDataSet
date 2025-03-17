import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Tworzymy parser argumentów
parser = argparse.ArgumentParser(description="Script to filter sales data")
parser.add_argument("--input_path", required=True, type=str, help="Path to the input CSV file")
parser.add_argument("--output_path", required=True, type=str, help="Path to save the filtered Parquet file")
parser.add_argument("--min_sales", required=True, type=int, help="Minimum sales threshold for filtering")

args = parser.parse_args()

spark = SparkSession.builder.appName("SalesFilter").getOrCreate()

df = spark.read.option("header", "true").csv(args.input_path)

df = df.withColumn("quantity_sold", col("quantity_sold").cast("int"))
df = df.withColumn("revenue", col("revenue").cast("int"))

df_filtered = df.filter(col("quantity_sold") >= args.min_sales)

df_sum = df_filtered.groupBy("product_name").agg(sum("revenue").alias("sum_revenue_per_product"))

df_filtered = df_filtered.join(df_sum, "product_name", "inner")

df_filtered = df_filtered.orderBy(col("sum_revenue_per_product").desc())

df_filtered.write.mode("overwrite").parquet(args.output_path)

