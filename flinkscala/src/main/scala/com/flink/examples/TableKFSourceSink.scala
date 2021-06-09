package com.flink.examples

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * kafka生产者:
 * kafka-console-producer.sh --broker-list hd1:9092,hd2:9092,hd3:9092 --topic sensor
 * kafka消费者：
 *  kafka-console-consumer.sh --bootstrap-server hd1:9092,hd2:9092,hd3:9092 --topic sensors
 */
object TableKFSourceSink extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //  val ds = env.readTextFile("input/sensor.txt")
  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings
    .newInstance
    .useOldPlanner
    .inStreamingMode
    .build)
  // 从kafka读取数据
  tenv.connect(
    new Kafka()
      //      .version("2.8.0")
      .version("universal")
      .topic("sensor")
      .property("zookeeper.connect", "hd1:2181,hd2:2181,hd3:2181")
      .property("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092")
  )
    //        .withFormat(new Json())
    .withFormat(new Csv())
    .withSchema(
      new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("sensor")


  // 输出数据到kafka
  tenv.connect(
    new Kafka().version("universal")
      .topic("sensors")
      .property("zookeeper.connect", "hd1:2181,hd2:2181,hd3:2181")
      .property("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092")
  )
    .withFormat(new Csv())
    .withSchema(
      new Schema()
        .field("id", DataTypes.STRING())
        //        .field("ts", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("sensors")
  // table api
  val resultTable = tenv
    .from("sensor").select('id, 'temperature)
    .filter('id === "ws_001")
    //    .insertInto("sensors")
    .executeInsert("sensors")
  //    env.execute
}