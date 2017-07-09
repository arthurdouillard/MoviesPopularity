package main.scala

import Processor.GenreProcessor
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

    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("/tmp/temp")
    ssc.sparkContext.addFile("analyser/sentimentAnalyser.py")

    val pyCmd = getPythonCmd()

    val streamRaw = setUpStream(brokers, topicFetch, ssc)
    streamRaw
      .map(_._2)
      .foreachRDD(rdd => {
        rdd.pipe(pyCmd).foreachPartition { partitionOfRecords =>
          partitionOfRecords.foreach(movie => sendTo(topicSave, brokers, movie))
        }
      })


    val streamSent = setUpStream(brokers, topicSave, ssc)
    val baseStream = streamSent.map(_._2)
                              .map(Json.parse(_).as[Movie])
                              .map(movie => movie.copy(sentimentScore = Some(calculateFinalScore(movie))))

    new GenreProcessor(brokers, "genre", baseStream).process()

    ssc.start()
    ssc.awaitTermination()

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
