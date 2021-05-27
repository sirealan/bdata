package com.atguigu.examples

import com.atguigu.bean.SensorReader
import org.apache.flink.streaming.api.scala._

object Test extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.socketTextStream("localhost", 9999)
    .map(o => {
      val split = o.split(",")
      SensorReader(
        split(0),
        split(1).toLong,
        split(2).toDouble
      )
    })
    .keyBy(_.id)
//    .flatMap(new AlertFlatMapFunc(10.0))
    //    .flatMap(_.split(","))
    .print
  env.execute
}