package com.flink.examples

import org.apache.flink.streaming.api.scala._

object AdCount extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.readTextFile("input/AdClickLog.csv")
    .map(o=>{
      val split = o.split(",")
//      Tuple2(Tuple2(split(2),split(1).toLong),1L)
      ((split(2),split(1).toLong),1L)
    })
    .keyBy(_._1)
    .sum(1)
    .print()
  env.execute()

}
