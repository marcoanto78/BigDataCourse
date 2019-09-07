from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

productSchema = StructType().add("id", "integer").add("name", "string")

lines = spark \
    .readStream \
    .option("sep", ",") \
    .schema(productSchema) \
    .csv("./folder/")

query = lines \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()