package main.scala

import java.util.Properties
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import java.util.Arrays;

case class Consumer(zooKeeper: String, groupId: String, waitTime: String) {

  val kafkaProps = new Properties()
  kafkaProps.put("zookeeper.connect", zooKeeper)
  kafkaProps.put("group.id", groupId)
  kafkaProps.put("auto.commit.interval.ms", "1000")
  kafkaProps.put("auto.offset.reset", "smallest");
  kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

  private val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaProps)


  def subscribe(topic: String): Unit = {
    consumer.subscribe(Arrays.asList(topic))
    System.out.println("Subscribed to topic " + topic);
  }

  def read(topic:String): String =  {
    /*val stream = streams.get(topic).get
    val it: ConsumerIterator[String, String] = stream.iterator()
    it.next().message()*/
    ???
  }

  def getConsumer(): KafkaConsumer[String, String] = {
    return this.consumer
  }

  //def shutdown(): Unit = consumer.shutdown()
}