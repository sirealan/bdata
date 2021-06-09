package com.flink.examples

import com.flink.bean.UserBehavior
import org.apache.flink.streaming.api.scala._

object WordCount extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.readTextFile("input/UserBehavior.csv")
    .map(o=>{
      val split = o.split(",")
      UserBehavior(
        split(0).toLong,
        split(1).toLong,
        split(2).toInt,
        split(3),
        split(4).toLong
      )
    })
    .filter(_.behavior.equals("pv"))
    .map(o=>("pv",1L))
    .keyBy(_._1)
    .sum(1)
//    .process(new KeyedProcessFunction[String,UserBehavior,Long]() {
//      var count = 0L;
//
//      override def processElement(i: UserBehavior, context: KeyedProcessFunction[String, UserBehavior, Long]#Context, collector: Collector[Long]): Unit = {
//        count = count+1
//        collector.collect(count)
//      }
//    })
    .print()
  env.execute()
}
