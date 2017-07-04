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

    System.out.println(args)

    if (args.length < 2) {
      System.err.println("Please specify the following arguments: <brokers_list> and <topics_list>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val sc = new SparkConf().setMaster("local[*]")
                           .setAppName("MoviesPopularity")
    val ssc = new StreamingContext(sc, Seconds(2))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val data = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    data.map(System.out.println(_))
  }
}
