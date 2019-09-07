name := "kafkasparkstreaming"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
