
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

checkpointDirectory = "./checkpoint"

def doYourOperations():
  print "########################### We are in doYourOperations #################################"
  sc = SparkContext("local[2]", "CheckpointDemo")
  ssc = StreamingContext(sc, 5)
  lines = ssc.socketTextStream("localhost", 9999)
  filtered = lines.filter(lambda x: "spam" in x)
  ssc.checkpoint(checkpointDirectory)  # set checkpoint directory
  filtered.pprint()
  return ssc

# Get StreamingContext from checkpoint data or create a new one
context = StreamingContext.getOrCreate(checkpointDirectory, doYourOperations)

context.start()             # Start the computation
context.awaitTermination()

