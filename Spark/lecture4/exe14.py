from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split as _split

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "54.68.153.139") \
    .option("port", 9999) \
    .load()

productSchema = StructType().add("product_id", "integer").add("name", "string").add("added_dt","string").add("deactivated_dt","string")

productDf = spark.read.csv("./product.csv", mode="DROPMALFORMED", schema=productSchema)

splittedClms = _split(lines.value, ",")
combined_df = lines.withColumn("tx_id",splittedClms.getItem(0).cast("integer")) \
     .withColumn("product_id",splittedClms.getItem(1).cast("integer")) \
     .withColumn("qty",splittedClms.getItem(2).cast("integer")) \
     .withColumn("amt",splittedClms.getItem(3).cast("integer")) \
     .withColumn("day_dt",splittedClms.getItem(4).cast("string")) 

streamingDf = combined_df.select("tx_id", "product_id", "qty", "amt", "day_dt")

joinedDf = streamingDf.join(productDf, "product_id")  # inner equi-join with a static DF
#streamingDf.join(staticDf, "type", "right_join")  

query = joinedDf \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
