package wordcount

import java.lang.Long
import java.util.Properties
import java.util.concurrent.CountDownLatch
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import scala.collection.JavaConverters.asJavaIterableConverter

object WordCount {

  def main(args: Array[String]) {
    println("In main")
    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()

    val textLines: KStream[String, String] = builder.stream("wc_input")

    /* val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
      .groupBy((_, word) => word)
      .count(Materialized.as("counts-store").asInstanceOf[Materialized[String, Long, KeyValueStore[Bytes, Array[Byte]]]])
    */

    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
      .groupBy((_, word) => word).count()

    wordCounts.toStream().to("wc_output", Produced.`with`(Serdes.String(), Serdes.Long()))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    val latch: CountDownLatch = new CountDownLatch(1)

    Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown-hook") {
      override def run(): Unit = {
        streams.close
        latch.countDown
      }
    })

    try {
      streams.start
      latch.await
    } catch {
      case e: Throwable =>
        System.exit(1)
    }

  }
}