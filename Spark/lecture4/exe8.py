from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

query = lines \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
