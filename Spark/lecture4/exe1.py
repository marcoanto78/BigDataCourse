import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if len(sys.argv) < 2:
  print "Please pass server ip as parameter"
  sys.exit(0)

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream(sys.argv[1], 9999)
lines.pprint()
ssc.start()
ssc.awaitTermination()
