package Processor

import main.scala.Producer
import model.{Movie, Review}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json

abstract class Processor(brokers: String, topic: String, baseStream: DStream[Movie]) {
  implicit val reviewFormat = Json.format[Review]
  implicit val movieFormat = Json.format[Movie]

  abstract def process(): Unit

  abstract def serialize(rdd: RDD[Any]): String

  abstract def update(newValues: Seq[Any], state: Option[Any]): Option[Any]

  def sendTo(value: String): Unit = {
    val producer = new Producer(topic, brokers)
    producer.send(value)
    producer.flush()
    producer.close()
  }
}
