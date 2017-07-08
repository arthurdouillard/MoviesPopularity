package main.scala

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import model.{Movie, Review}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import play.api.libs.json.Json


object Main {

  implicit val reviewFormat = Json.format[Review]
  implicit val movieFormat = Json.format[Movie]


  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Please specify the following arguments: <brokers_list>  <fetch_topic> <save_topic>")
      System.exit(1)
    }


    val Array(brokers, topicFetch, topicSave) = args
    val topicSentiment = "sentiment"

    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.sparkContext.addFile("analyser/sentimentAnalyser.py")

    val pyCmd = getPythonCmd()

    val streamRaw = setUpStream(brokers, topicFetch, ssc)
    streamRaw
      .map(_._2)
      .foreachRDD(rdd => {
        rdd.pipe(pyCmd).foreach(movie => {
          sendTo(topicSentiment, brokers, movie)
          sendTo(topicSave, brokers, movie)
        })
      })


    val streamSent = setUpStream(brokers, topicSentiment, ssc)
    streamSent
      .map(_._2)
      .map(Json.parse(_).as[Movie])
      .map(movie => movie.copy(sentimentScore = Some(calculateFinalScore(movie))))
      .foreachRDD(rdd => rdd.foreach(println))

    /*
      .foreachRDD(rdd => rdd.foreach(movie => {
        sendTo("genre", brokers, Json.toJson(movie).toString())
      }))
*/

  /*
    val streamGenre = setUpStream(brokers, "genre", ssc)
    val tot = streamGenre.updateStateByKey[Float]((v: Seq[String], c: Option[Float]) => {
      val t = v.head
      val j = Json.parse(t).as[Movie]
      val prev : Float = c.getOrElse(0)
      val score = j.score
      val current = prev + score

      Some(current)
    })

    tot.map(a => println(a._1, a._2))
*/
      /*
      .map(_._2)
      .map(Json.parse(_).as[Movie])
      .updateStateByKey[Map[String, Float]]()
*/
    ssc.start()
    ssc.awaitTermination()
  }


  def update(i: Int, count: Option[Int]) = {
    val prev = count.getOrElse(0)
    val current = prev + i
    Some(current)
  }

  def sendTo(topic: String, brokers: String, value: String): Unit = {
    val producer = new Producer(topic, brokers)
    producer.send(value)
    producer.flush()
    producer.close()
  }


  def getPythonCmd(): String = {
    val cmdClf = " --clf " + "analyser/classifier.pkl"
    "python3 " + SparkFiles.get("sentimentAnalyser.py") + cmdClf

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
    val total = movie.reviews.foldRight(0)((review, counter) => counter + review.sentiment)
    total * 10 / movie.reviews.length
  }

}
