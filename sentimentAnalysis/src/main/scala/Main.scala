package main.scala

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import model.{Movie, Review}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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

    /*
     * ---------------------------
     *    1. Set up of the different config need by Spark, Spark-streaming, and Kafka.
     * ---------------------------
     */
    val sc = new SparkConf().setAppName("MoviesPopularity")
                            .setMaster("local[*]")
                            .set("spark.streaming.concurrentJobs", "5")
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("/tmp/temp")
    ssc.sparkContext.addFile("analyser/sentimentAnalyser.py")


    /*
     * ---------------------------
     *    1. Get the raw stream from the fetcher.
     *    2. Process the reviews through the sentiment analyser.
     *    3. Incorporate the predicted sentiments in the data.
     * ---------------------------
     */
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

    /*
     * ---------------------------
     *    Genres processing
     *    Send a serialized map of [GenreName -> GenreAverage].
     *    The average is based on the sentimental score.
     * ---------------------------
     */

    processGenre(brokers, "genre", baseStream) // Map[String, Int]
    processDirector(brokers, "director", baseStream) // Map[String, Int]
    processActor(brokers, "actors", baseStream) // Map[String, Int]
    processScore(brokers, "score", baseStream) // Map[String, List[Int]]
    processYear(brokers, "years", baseStream) // Map[String, Int] /!\ Convert the key from string to int !

    ssc.start()
    ssc.awaitTermination()

  }


  case class AvgHolder(sum: Float, count: Int) {
    val avg = (sum / scala.math.max(1, count)).toInt

    def +(sum: Float, count: Int): AvgHolder = AvgHolder(
      this.sum + sum,
      this.count + count
    )
  }

  def processYear(brokers: String, topic: String, stream: DStream[Movie]): Unit = {

    def serializeYear(rdd: RDD[(Int, AvgHolder)]): String = {
      val data = rdd
        .takeOrdered(10)(Ordering[Int].reverse.on(x => x._2.avg))
        .map(tuple => tuple._1.toString -> tuple._2.avg).toMap

      Json.stringify(Json.toJson(data))
    }

    def updateYear(newValues: Seq[Option[Float]], state: Option[AvgHolder]): Option[AvgHolder] = {
      val prev = state.getOrElse(AvgHolder(0, 0))
      val values = newValues.flatMap(x => x)
      val current = prev + (values.sum, values.size)
      Some(current)
    }


    stream.map(movie => (movie.year, movie.sentimentScore))
      .updateStateByKey(updateYear)
      .foreachRDD(rdd => {
        if (!rdd.isEmpty)
          sendTo(topic, brokers, serializeYear(rdd))
      })
  }
  // ---------------------------------------


  // ---------------------------------------
  def processScore(brokers: String, topic: String, stream: DStream[Movie]): Unit = {
    case class AvgPairHolder(sum1: Float, sum2: Float, count: Int) {
      val avg1 = (sum1 / scala.math.max(1, count)).toInt
      val avg2 = (sum2 / scala.math.max(1, count)).toInt


      def +(sum1: Float, sum2: Float, count: Int) = AvgPairHolder(
        this.sum1 + sum1,
        this.sum2 + sum2,
        this.count + count
      )
    }

    case class ScorePair(v1: Float, v2: Float)

    def serializeScore(rdd: RDD[(String, AvgPairHolder)]): String = {
      val data = rdd
        .takeOrdered(10)(Ordering[Int].reverse.on(x => x._2.avg1))
        .map(tuple => tuple._1 -> List[Int](tuple._2.avg1, tuple._2.avg2)).toMap

      Json.stringify(Json.toJson(data))
    }

    def updateScore(newValues: Seq[ScorePair], state: Option[AvgPairHolder]): Option[AvgPairHolder] = {
      val prev = state.getOrElse(AvgPairHolder(0, 0, 0))
      val sum1 = newValues.foldRight(0.0)((x, c) => x.v1 + c).toFloat
      val sum2 = newValues.foldRight(0.0)((x, c) => x.v2 + c).toFloat
      val current = prev + (sum1, sum2, newValues.size)
      Some(current)
    }

    stream.filter(_.sentimentScore.isDefined)
      .flatMap(movie => {
        for (genre <- movie.genres) yield (genre, ScorePair(movie.score, movie.sentimentScore.get))
      })
      .updateStateByKey(updateScore)
      .foreachRDD(rdd => {
        if (!rdd.isEmpty)
          sendTo(topic, brokers, serializeScore(rdd))
      })
  }
  // ---------------------------------------

  // ---------------------------------------
  def processActor(brokers: String, topic: String, stream: DStream[Movie]): Unit = {

    def serializeActor(rdd: RDD[(String, AvgHolder)]): String = {
      val data = rdd
        .takeOrdered(10)(Ordering[Int].reverse.on(x => x._2.avg))
        .map(tuple => tuple._1 -> tuple._2.avg).toMap

      Json.stringify(Json.toJson(data))
    }

    def updateActor(newValues: Seq[Option[Float]], state: Option[AvgHolder]): Option[AvgHolder] = {
      val prev = state.getOrElse(AvgHolder(0, 0))
      val values = newValues.flatMap(x => x)
      val current = prev + (values.sum, values.size)
      Some(current)
    }


    stream.flatMap(movie => {
                    for (actor <- movie.actors) yield (actor, movie.sentimentScore)
            })
          .updateStateByKey(updateActor)
          .foreachRDD(rdd => {
            if (!rdd.isEmpty)
              sendTo(topic, brokers, serializeActor(rdd))
          })
  }
  // ---------------------------------------

  // ---------------------------------------
  def processDirector(brokers: String, topic: String, stream: DStream[Movie]): Unit = {

    def serializeDirector(rdd: RDD[(String, AvgHolder)]): String = {
      val data = rdd
        .takeOrdered(10)(Ordering[Int].reverse.on(x => x._2.avg))
        .map(tuple => tuple._1 -> tuple._2.avg).toMap

      Json.stringify(Json.toJson(data))
    }

    def updateDirector(newValues: Seq[Option[Float]], state: Option[AvgHolder]): Option[AvgHolder] = {
      val prev = state.getOrElse(AvgHolder(0, 0))
      val values = newValues.flatMap(x => x)
      val current = prev + (values.sum, values.size)
      Some(current)
    }


    stream.filter(_.director.isDefined)
        .map(movie => (movie.director.get, movie.sentimentScore))
        .updateStateByKey(updateDirector)
        .foreachRDD(rdd => {
          if (!rdd.isEmpty)
            sendTo(topic, brokers, serializeDirector(rdd))
        })

  }
  // ---------------------------------------


  // ---------------------------------------
  def processGenre(brokers: String, topic: String, stream: DStream[Movie]): Unit = {

    def serializeGenre(rdd: RDD[(String, AvgHolder)]): String = {
      val genres = rdd
        .map(tuple => Map(tuple._1 -> tuple._2.avg))
        .collect()
        .reduce(_ ++ _)

      Json.stringify(Json.toJson(genres))
    }

    def updateGenre(newValues: Seq[Option[Float]], state: Option[AvgHolder]): Option[AvgHolder] = {
      val prev = state.getOrElse(AvgHolder(0, 0))
      val values = newValues.flatMap(x => x)
      val current = prev + (values.sum, values.size)
      Some(current)
    }


    stream.flatMap(movie => {
                    for (genre <- movie.genres) yield (genre, movie.sentimentScore)
                  })
                  .updateStateByKey(updateGenre)
                  .foreachRDD(rdd => {
                    if (!rdd.isEmpty)
                      sendTo(topic, brokers, serializeGenre(rdd))
                  })
  }
  // ---------------------------------------


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
