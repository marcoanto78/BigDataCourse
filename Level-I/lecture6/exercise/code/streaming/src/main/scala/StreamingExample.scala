import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object StreamingExample {
  def main(args: Array[String]):Unit = {
    val numReceivers = 2 //note: has to be >= 2
    val conf = new SparkConf().setMaster(s"local[${numReceivers}]").setAppName("YetAnotherWordCountExample")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val wordCounts = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    wordCounts.print() //output operation is this

    ssc.start()
    ssc.awaitTermination()
  }
}
