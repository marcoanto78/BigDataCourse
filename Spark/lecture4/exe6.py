import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if len(sys.argv) < 2:
  print "Please pass server ip as parameter"
  sys.exit(0)

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream(sys.argv[1], 9999)

#Generate key value pairs
keyValRdd = lines.map(lambda x: (x.split(",")[1],x))

#Read product RDD in memory
productRdd = sc.textFile("/home/s_kante/gitrepo/big_data_course/Spark/lecture4/product.csv")
productKeyVal = productRdd.map(lambda x: (x.split(",")[0],x))

#Join with dimension table to make sure we are excluding records not present in dimension table
joinedRdd = keyValRdd.transform(lambda rdd: rdd.join(productKeyVal))

(u'41', (u'14,41,10,67,2019-02-02 12:12:12', u'41,Keeping It Local,2017-02-10,'))
(u'56', (u'11,56,10,6,2019-02-02 12:12:12', u'56,Polenta,2017-02-25,'))

#Get only required fields and print the result for demo purpose
finalRdd = joinedRdd.map(lambda x: x[1][0])
finalRdd.pprint()

ssc.start()
ssc.awaitTermination()
