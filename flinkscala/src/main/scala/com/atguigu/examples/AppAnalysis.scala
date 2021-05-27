package com.atguigu.examples

import cn.hutool.core.thread.ThreadUtil
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit
import scala.util.Random

object AppAnalysis extends App {
  import org.apache.flink.streaming.api.scala._
  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  import org.apache.flink.api.scala._
//  val env = ExecutionEnvironment.getExecutionEnvironment
//    ExecutionEnvironment.getExecutionEnvironment
//    .setRuntimeMode(RuntimeExecutionMode.STREAMING)
  env.setParallelism(1)
  env
    .readTextFile("input/appanalysis.txt")
//    .readTextFile("input/a.txt")
//        .addSource(new AppMarketingDataSource)
    .map(o => {
      val split = o.split(",")
//      MarketingUserBehavior(
      (
        split(0).toLong,
        split(1),
        split(2),
        split(3).toLong
      )
    })
    .keyBy(o => Tuple2(o._3, o._2))
//        .keyBy(o => (o.channel, o.behavior))
        .process(
          new KeyedProcessFunction[Tuple2[String, String], (Long, String, String, Long),
            Tuple2[Tuple2[String, String], Int]]() {
            val hashMap = collection.mutable.HashMap[String,Int]()

            override def processElement(i: (Long, String, String, Long), context: KeyedProcessFunction[(String, String), (Long, String, String, Long), ((String, String), Int)]#Context, collector: Collector[((String, String), Int)]): Unit = {
              val hashKey = i._3.concat("-").concat(i._2)
              var count = hashMap.getOrElse(hashKey,0)
              count = count+1
              collector.collect(Tuple2(context.getCurrentKey, count))
              hashMap.update(hashKey, count)
            }
      })
//    .map(
//      _.toString.replace("(", "").replace(")", "")
//    )
    .print()
  env.execute


  //  class AppMarketingDataSource extends RichSourceFunction[MarketingUserBehavior] {
  class AppMarketingDataSource extends RichSourceFunction[(Long, String, String, Long)] {
    var flag = true
    val random = new Random
    var channels = collection.mutable.MutableList[String]("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo")
    var behaviors = collection.mutable.MutableList[String]("download", "install", "update", "uninstall")

    //    override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    override def run(sourceContext: SourceFunction.SourceContext[(Long, String, String, Long)]): Unit = {
      while (flag) {
        sourceContext.collect(
          //          MarketingUserBehavior
          (
            random.nextInt(1000000).toLong,
            behaviors.get(random.nextInt(behaviors.size)).get,
            channels.get(random.nextInt(channels.size)).get,
            System.currentTimeMillis
          ))
        ThreadUtil.sleep(1, TimeUnit.NANOSECONDS)
      }
    }

    override def cancel(): Unit = flag = false
  }
}