package com.atguigu.examples

//import org.apache.flink.api.scala._
import com.atguigu.bean.UserBehavior
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object UV extends App {

  val env =
//    ExecutionEnvironment.getExecutionEnvironment
    StreamExecutionEnvironment.getExecutionEnvironment
//      .setRuntimeMode(RuntimeExecutionMode.BATCH)
  env
    .readTextFile("input/UserBehavior.csv")
    .flatMap(new FlatMapFunction[String, UserBehavior]() {
      override def flatMap(t: String, collector: Collector[UserBehavior]): Unit = {
        val split = t.split(",")
        split(3) match {
          case "pv" => collector.collect(UserBehavior(
            split(0).toLong,
            split(0).toLong,
            split(0).toInt,
            split(0),
            split(0).toLong
          ))
          case _ =>
        }
      }
    })
    .keyBy(d=>"d")
    .process(new KeyedProcessFunction[String,UserBehavior,Int] {
      var count = 0
      val uids = collection.mutable.HashSet.empty[Long]
      override def processElement(i: UserBehavior, context: KeyedProcessFunction[String, UserBehavior, Int]#Context, collector: Collector[Int]): Unit = {
        uids(i.userId) match {
          case false => {
            uids+=i.userId
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
