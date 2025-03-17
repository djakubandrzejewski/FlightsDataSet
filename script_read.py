import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description="Script to read first n rows from a CSV file")
parser.add_argument("--file", required=True, type=str, help="Path to the CSV file")
parser.add_argument("--rows", required=False, type=int, default=5, help="Number of rows to show (default: 5)")

args = parser.parse_args()

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.option("header", "true").csv(args.file)

def how_many_rows(dataframe, rows):
    dataframe.show(rows)

how_many_rows(df, args.rows)
