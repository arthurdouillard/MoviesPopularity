package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder

/**
  * @author douill_a
  * @date 27/06/2017
  */
object Main {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Please specify the following arguments: <brokers_list> and <topics_list>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    System.out.println(brokers)
    System.out.println(topics)
    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(1))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("bootstrap.servers" -> brokers)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
