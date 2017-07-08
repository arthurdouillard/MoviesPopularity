package main.scala

import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}


class Producer(topic: String, brokerList:String) {
  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", brokerList)

  // This is mandatory, even though we don't send keys
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("acks", "all")

  // how many times to retry when produce request fails?
  kafkaProps.put("retries", "3")
  kafkaProps.put("linger.ms", "5")
  kafkaProps.put("producer.type", "async")

  //this is our actual connection to Kafka!
  private val producer = new KafkaProducer[String, String](kafkaProps)

  def send(value: String): Unit = {
    val record = new ProducerRecord[String, String](topic, value)
    try {
      producer.send(record).get()
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }

  def close():Unit = producer.close()

  def flush():Unit = producer.flush()
}