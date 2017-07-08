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

  implicit val movieFormat = Json.format[Movie]

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Please specify the following arguments: <brokers_list>  <topics_list> <hdfs_path>")
      System.exit(1)
    }


    val Array(brokers, topic, hdfsPath) = args

    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.sparkContext.addFile("analyser/sentimentAnalyser.py")

    val pyCmd = getPythonCmd()

    val sentimentTopic = "sentiment"

    val streamRaw = setUpStream(brokers, topic, ssc)
    streamRaw
      .map(_._2)
      .foreachRDD(rdd => {
        rdd.pipe(pyCmd).foreach(movie => {
          val producer = new Producer("sentiment", brokers)
          producer.send(movie)
          producer.flush()
          producer.close()
        })
      })


    val streamSent = setUpStream(brokers, sentimentTopic, ssc)
    streamSent
      .map(_._2)
      .map(Json.parse(_).as[Movie])
      .foreachRDD(rdd => rdd.foreach(println))
     // .saveAsTextFiles(hdfsPath, "txt")

    ssc.start()
    ssc.awaitTermination()
  }


  def getPythonCmd(): String = {
    val cmdClf = " --clf " + "analyser/classifier.pkl"
    return "python3 " + SparkFiles.get("sentimentAnalyser.py") + cmdClf

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
