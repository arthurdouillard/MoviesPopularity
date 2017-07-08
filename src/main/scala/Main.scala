package main.scala

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import model.{Movie, Review}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import play.api.libs.json.Json


object Main {

  implicit val movieFormat = Json.format[Movie]

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Please specify the following arguments: <brokers_list>  <topics_list> <hdfs_path>")
      System.exit(1)
    }


    val Array(brokers, topic, hdfsPath) = args
    val sentTopic = "sentiment"

    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(2))

    val streamRaw = setUpStream(brokers, topic, ssc)


    ssc.sparkContext.addFile("sentimentAnalysis/sentimentAnalyser.py")

    streamRaw.map(_._2)
      .foreachRDD(rdd => analyseSentiment(sentTopic, brokers, rdd))

    val streamSent = setUpStream(brokers, sentTopic, ssc)
    streamSent.map(_._2)
      .map(Json.parse(_).as[Movie])
      .map(movie => (movie, calculateFinalScore(movie)))
      .saveAsTextFiles(hdfsPath, "txt")

    ssc.start()
    ssc.awaitTermination()
  }

  def analyseSentiment(topic: String, brokers: String, rdd: RDD[String]) = {
    val cmdTopic = " --topic " + topic
    val cmdBrokers = " --brokers " + brokers
    val cmdClf = " --clf " + "sentimentAnalysis/classifier.pkl"
    rdd.pipe("python3 " + SparkFiles.get("sentimentAnalyser.py") + cmdTopic + cmdBrokers + cmdClf)
  }

  def setUpStream(brokers: String, topic: String, ssc: StreamingContext): InputDStream[(String, String)] = {



    val kafkaParams = Map[String, String]("bootstrap.servers" -> brokers)

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        Set(topic)
    )
  }


  def calculateFinalScore(movie: Movie): Float = {
    var total = 0
    for (r <- movie.reviews) {
      if (r.sentiment != 0) total += 1
    }

    return total * 10 / movie.reviews.length
  }

}
