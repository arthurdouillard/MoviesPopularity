package Processor

import model.Movie
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json

/**
  * @author douill_a
  * @date 09/07/2017
  */
class GenreProcessor(brokers: String, dstTopic: String, baseStream: DStream[Movie])
  extends Processor(brokers: String, dstTopic: String, baseStream: DStream[Movie]) {

  case class Genre(sum: Float, count: Int) {
    val avg = (sum / scala.math.max(1, count)).toInt

    def +(sum: Float, count: Int): Genre = Genre (
      this.sum + sum,
      this.count + count
    )

    override def toString = s"Avg: $avg for $count movies."
  }

  def process(): Unit = {
    baseStream.flatMap(movie => {
                  for (genre <- movie.genres) yield (genre, movie.sentimentScore)
                })
                .updateStateByKey(update)
                .foreachRDD(rdd => {
                  if (!rdd.isEmpty)
                    sendTo(serialize(rdd))
                })
  }

  def serialize(rdd: RDD[(String, Genre)]): String = {
    val genres = rdd
      .map(tuple => Map(tuple._1 -> tuple._2.avg))
      .collect()
      .reduce(_ ++ _)

    Json.toJson(genres).toString()
  }

  def update(newValues: Seq[Option[Float]], state: Option[Genre]): Option[Genre] = {
    val prev = state.getOrElse(Genre(0, 0))
    val values = newValues.flatMap(x => x)
    val current = prev + (values.sum, values.size)
    Some(current)
  }

}
