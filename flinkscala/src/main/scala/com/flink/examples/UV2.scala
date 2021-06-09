package com.flink.examples

//import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object UV2 extends App {

  val env =
    StreamExecutionEnvironment.getExecutionEnvironment
//      .setRuntimeMode(RuntimeExecutionMode.BATCH)
  env
    .readTextFile("input/UserBehavior.csv")
    .flatMap(new FlatMapFunction[String, String]() {
      override def flatMap(t: String, collector: Collector[String]): Unit = {
        val split = t.split(",")
        split(3) match {
          case "pv" => collector.collect(split(0))
          case _ =>
        }
      }
    })
    .keyBy(d=>"d")
    .process(new KeyedProcessFunction[String,String,Int] {
      var count = 0
      val uids = collection.mutable.HashSet.empty[String]
      override def processElement(i: String, context: KeyedProcessFunction[String, String, Int]#Context, collector: Collector[Int]): Unit = {
        uids(i) match {
          case false => {
            uids+=i
            count = count+1
            collector.collect(count)
          }
          case _ =>
        }
      }
    })

    .print

  env.execute


}
