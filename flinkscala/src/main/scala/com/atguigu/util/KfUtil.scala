package com.atguigu.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KfUtil {
  val propertis = new Properties
  val bootservers = "hd1:9092,hd2:9092,hd3:9092"
  val zookeepers = "hd1:2181,hd2:2181,hd3:218"

  def source(topic: String): FlinkKafkaConsumer[String] = {
    propertis.clear
    propertis.setProperty("bootstrap.servers", bootservers)
    //  propertis.setProperty("group.id", "consumer-group")
    propertis.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propertis.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    propertis.setProperty("auto.offset.reset", "latest")
    val fkc = new FlinkKafkaConsumer[String](topic,
      new SimpleStringSchema(), propertis)
    fkc
  }

  /**
   * 读取文件写到kafka
   *
   * @param topic
   */
  //  def sink(topic: String): KafkaProducer[String, String] = {
  def write(topic: String): Unit = {
    propertis.clear
    propertis.setProperty("bootstrap.servers", bootservers)
    //  propertis.setProperty("group.id", "producer-group")
    propertis.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    propertis.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //    propertis.setProperty("auto.offset.reset", "latest")
    val product = new KafkaProducer[String, String](propertis)
    scala.io.Source
      .fromFile("/Users/alan/IdeaProjects/flink-200821/input/UserBehavior.csv")
      .getLines.foreach(o => {
      val record = new ProducerRecord[String, String](topic, o)
      product.send(record)
    })
    product.close
    //    product
  }
}
