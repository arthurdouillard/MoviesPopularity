package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


/**
  * @author douill_a
  * @date 27/06/2017
  */
object Main {
  def main(args: Array[String]) {
    val sc = new SparkConf().setAppName("MoviesPopularity")

    val producer = new Producer(args(0), args(1))
    val ssc = new StreamingContext(sc, Seconds(2))
  }
}
