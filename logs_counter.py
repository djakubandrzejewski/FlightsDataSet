from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("LogCounter").getOrCreate()

df = spark.read.text("/content/logs.txt")

df = df.withColumn("log_clean", F.split(df["value"], " ")[2])

df_result = df.groupBy(
    F.col("log_clean")
).agg(
    F.count("*").alias("count_log")
)

df_result.write.format("csv").option("header","true").save("/content/result")