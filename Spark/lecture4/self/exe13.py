
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

df = lines.selectExpr( "'test' as topic", "'1' as key", "value" )

query = df \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "ec2-54-68-153-139.us-west-2.compute.amazonaws.com:9092") \
  .option("checkpointLocation", "./checkpoint") \
  .start()

query.awaitTermination()

#Note: Make sure to delete checkpointLocation used before
