name := "MoviesPopularity"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0",
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.1",
  "com.typesafe.play" %% "play-json" % "2.4.0"

)
    