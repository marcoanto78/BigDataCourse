from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

productSchema = StructType().add("id", "integer").add("name", "string")

lines = spark \
    .readStream \
    .option("sep", ",") \
    .schema(productSchema) \
    .csv("./folder/")

finalop = lines \
    .writeStream \
    .format("csv") \
    .option("sep",",") \
    .option("path", "./output/demo_op") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

finalop.awaitTermination()