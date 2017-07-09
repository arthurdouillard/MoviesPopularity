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
    val sc = new SparkConf().setAppName("MoviesPopularity").setMaster("local[2]")
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

    processGenre(brokers, "genre", baseStream)
    processDirector(brokers, "director", baseStream)
    processActor(brokers, "actors", baseStream)

    ssc.start()
    ssc.awaitTermination()

  }

  // ---------------------------------------
  def processActor(brokers: String, topic: String, stream: DStream[Movie]): Unit = {
    case class Actor(sum: Float, count: Int) {
      val avg = (sum / scala.math.max(1, count)).toInt

      def +(sum: Float, count: Int): Actor = Actor(
        this.sum + sum,
        this.count + count
      )
    }

    def serializeDirector(rdd: RDD[(String, Actor)]): String = {
      val genres = rdd
        .takeOrdered(100)(Ordering[Int].reverse.on(x => x._2.avg))
        .map(tuple => tuple._1 -> tuple._2.avg).toMap

      Json.stringify(Json.toJson(genres))
    }

    def updateDirector(newValues: Seq[Option[Float]], state: Option[Actor]): Option[Actor] = {
      val prev = state.getOrElse(Actor(0, 0))
      val values = newValues.flatMap(x => x)
      val current = prev + (values.sum, values.size)
      Some(current)
    }


    stream.flatMap(movie => {
                    for (actor <- movie.actors) yield (actor, movie.sentimentScore)
            })
          .updateStateByKey(updateDirector)
          .foreachRDD(rdd => {
            if (!rdd.isEmpty)
              sendTo(topic, brokers, serializeDirector(rdd))
          })
  }
  // ---------------------------------------

  // ---------------------------------------
  def processDirector(brokers: String, topic: String, stream: DStream[Movie]): Unit = {
    case class Director(sum: Float, count: Int) {
      val avg = (sum / scala.math.max(1, count)).toInt

      def +(sum: Float, count: Int): Director = Director(
        this.sum + sum,
        this.count + count
      )
    }

    def serializeDirector(rdd: RDD[(String, Director)]): String = {
      val genres = rdd
        .takeOrdered(100)(Ordering[Int].reverse.on(x => x._2.avg))
        .map(tuple => tuple._1 -> tuple._2.avg).toMap

      Json.stringify(Json.toJson(genres))
    }

    def updateDirector(newValues: Seq[Option[Float]], state: Option[Director]): Option[Director] = {
      val prev = state.getOrElse(Director(0, 0))
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
    case class Genre(sum: Float, count: Int) {
      val avg = (sum / scala.math.max(1, count)).toInt

      def +(sum: Float, count: Int): Genre = Genre(
        this.sum + sum,
        this.count + count
      )
    }

    def serializeGenre(rdd: RDD[(String, Genre)]): String = {
      val genres = rdd
        .map(tuple => Map(tuple._1 -> tuple._2.avg))
        .collect()
        .reduce(_ ++ _)

      Json.stringify(Json.toJson(genres))
    }

    def updateGenre(newValues: Seq[Option[Float]], state: Option[Genre]): Option[Genre] = {
      val prev = state.getOrElse(Genre(0, 0))
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
