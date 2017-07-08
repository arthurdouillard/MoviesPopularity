package main.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream


object Main {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Please specify the following arguments: <brokers_list> <topics_list> <hdfs_path>")
      System.exit(1)
    }

    val Array(brokers, topic, hdfsPath) = args
    val sc = new SparkConf().setAppName("MovieSaver").setMaster("local[*]")
    val ssc = StreamingContext.getActiveOrCreate(() => new StreamingContext(sc, Seconds(2)))

    val stream = setUpStream(brokers, topic, ssc)
    stream.saveAsTextFiles(hdfsPath, "txt")

    ssc.start()
    ssc.awaitTermination()
  }

  def setUpStream(brokers: String, topic: String, ssc: StreamingContext): InputDStream[(String, String)] = {
    val kafkaParams = Map[String, String]("bootstrap.servers" -> brokers)

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic)
    )
  }

}