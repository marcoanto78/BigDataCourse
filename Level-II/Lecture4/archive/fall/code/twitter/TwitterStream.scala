package twitterstreaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.twitter._
import org.apache.spark.{ SparkContext, SparkConf }


object TwitterStream {
  val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming - PopularHashTags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Set the Spark StreamingContext to create a DStream for every 5 seconds
    val ssc = new StreamingContext(sc, Seconds(20))
    // Pass the filter keywords as arguements

    //  val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Split the stream on space and extract hashtags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Get the top hashtags over the previous 10 sec window
    val topCounts20 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(20))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // print tweets in the currect DStream
    //stream.print()

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    topCounts20.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

//Reference: https://github.com/stdatalabs/SparkTwitterStreamAnalysis/blob/master/src/main/scala/com/stdatalabs/Streaming/SparkPopularHashTags.scala

