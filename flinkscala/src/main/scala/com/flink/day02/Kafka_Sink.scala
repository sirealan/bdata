package com.flink.day02

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties

object Kafka_Sink extends App {

  import org.apache.flink.streaming.api.scala._


  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties
  properties.setProperty("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092")
  properties.setProperty("group.id", "testKafka")
  val aa =
    env
      .fromCollection(
        List(
          WaterSensor("sensor_1", 1607527992000L, 20),
          WaterSensor("sensor_1", 1607527994000L, 50),
          WaterSensor("sensor_1", 1607527996000L, 30),
          WaterSensor("sensor_2", 1607527993000L, 10),
          WaterSensor("sensor_2", 1607527995000L, 30)
        )
      )
      .map(_.toString)
      .addSink(
        new FlinkKafkaProducer[String](
          "hd1:9092,hd2:9092,hd3:9092",
          "test",
          new SimpleStringSchema(),
        )
      )

  //    .flatMap(raw => JsonMethods.parse(raw).toOption)
  //    .map(_.extract[WaterSensor])

  env.execute

}
