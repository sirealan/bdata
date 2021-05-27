package com.atguigu.examples

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * kafka生产者:
 * kafka-console-producer.sh --broker-list hd1:9092,hd2:9092,hd3:9092 --topic sensor
 */
object TableFileOutput extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //  val ds = env.readTextFile("input/sensor.txt")
  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings
    .newInstance
    .useOldPlanner
    .inStreamingMode
    .build)
  // 从本地文件读取数据
  tenv.connect(new FileSystem().path("input/sensor.txt"))
    .withFormat(new Csv())
    .withSchema(
      new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("sensor")

  // 输出到文件
  tenv.connect(new FileSystem().path("out"))
    .withFormat(new Csv())
    .withSchema(
      new Schema()
        .field("id", DataTypes.STRING())
        //        .field("ts", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
    ).createTemporaryTable("sensorOut")
  // table api
  val resultTable = tenv
    .from("sensor").select('id, 'temperature)
    .filter('id === "ws_001")
    .insertInto( "sensorOut")
//    .executeInsert("sensorOut")
  // 1.控制台输出
//  resultTable.toAppendStream[(String,Double)].print("result")
//  resultTable.groupBy('id).select('id,'temperature.count() as 'count)
//    .toRetractStream[(String,Long)].print("agg")

//  resultTable.insertInto("sensorOut")
//  resultTable.
//  env.execute("")
}