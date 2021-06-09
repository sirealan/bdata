package com.flink.day01

import org.apache.flink.streaming.api.scala._

object WordCount_Bounded extends App {
  val env = StreamExecutionEnvironment
    .getExecutionEnvironment
    env
//      .readTextFile("input/word.txt")
      .socketTextStream("localhost", 9999)
    .setParallelism(1)
    .flatMap(_.split(" "))
    .map((_,1))
//    .keyBy(0)
    .keyBy(_._1)
    .sum(1)
    .print()

env.execute("hello stream")
}
