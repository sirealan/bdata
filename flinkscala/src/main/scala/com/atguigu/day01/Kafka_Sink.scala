package com.atguigu.day01

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object Kafka_Sink extends App {

  val properties = new Properties
  properties.setProperty("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092")
  properties.setProperty("group.id", "Flink_Source_Kafka")
  properties.setProperty("auto.offset.reset", "latest")

//properties.
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.addSource(
    new FlinkKafkaConsumer[String](
      "test",
      new SimpleStringSchema,
      properties
    )
  ).setParallelism(8)
    .print

  env.execute("flink_kafka")

}
