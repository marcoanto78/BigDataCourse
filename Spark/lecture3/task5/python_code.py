from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
  #Initialize the spark session
  spark = SparkSession \
        .builder \
        .appName("pyspark script job") \
        .getOrCreate()
  print "####################" + spark.conf.get("spark.driver.memory") + "####################"
  spark.stop()

