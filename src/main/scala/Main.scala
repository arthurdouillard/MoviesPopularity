package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import model.{Movie, Review}
import play.api.libs.json.Json

/**
  * @author douill_a
  * @date 27/06/2017
  */
object Main {

  implicit val movieFormat = Json.format[Movie]

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Please specify the following arguments: <brokers_list> and <topics_list>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(2))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("bootstrap.servers" -> brokers)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    stream.map(_._2)
          .map(Json.parse(_).as[Movie])
          .map(x => (x, calculateFinalScore(x)))
          .print()

    ssc.start()
    ssc.awaitTermination()
  }

  def calculateFinalScore(movie: Movie): Float = {
    var total = 0
    for (r <- movie.reviews) {
      if (r.sentiment != 0) total += 1
    }

    return total * 10 / movie.reviews.length
  }
}
