
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 2)

lines = ssc.socketTextStream("localhost", 9999)

#lines.foreachRDD(lambda x : x.saveAsTextFile("/mnt/d/exe4"))
lines.saveAsTextFiles("/mnt/d/exe4")

ssc.start()             # Start the computation
ssc.awaitTermination() 
