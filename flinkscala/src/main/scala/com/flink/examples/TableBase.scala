package com.flink.examples

import com.flink.bean.SensorReader
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

object TableBase extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val ds = env.readTextFile("input/sensor.txt")
    .map(o => {
      val split = o.split(",")
      SensorReader(
        split(0),
        split(1).toLong,
        split(2).toDouble
      )
    })
  val tenv = StreamTableEnvironment.create(env)
  val tt = tenv.fromDataStream(ds)
  tt
        .select("id,temperature")
//    .select('id, 'temperature)
    .filter("id=='ws_002'")
//    .filter('id=="ws_002")
    .toAppendStream[(String, Double)]
    .print("ds")
  tenv.createTemporaryView("tt", tt)
  tenv.sqlQuery("select id,temperature from tt where id = 'ws_002'")
    .toAppendStream[(String, Double)]
    .print("sql")
  env.execute
}