package com.atguigu.day01

import org.apache.flink.api.scala._

object WordCount_Batch extends App {
  ExecutionEnvironment
    .getExecutionEnvironment
    .readTextFile("input/word.txt")
    .setParallelism(8)
    .flatMap(_.split(" "))
    .map((_,1))
    .groupBy(0)
    .sum(1)
    .print()


}
