package com.flink.examples

import cn.hutool.core.thread.ThreadUtil
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit
import scala.util.Random

object AppAnalysis2 extends App {

  import org.apache.flink.streaming.api.scala._

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env
    .readTextFile("input/appanalysis.txt")
    .map(o => {
      val split = o.split(",")
      (split(0), split(1), split(2), split(3))
    })
    .keyBy(o => Tuple2(o._3, o._2))
    .process(new KeyedProcessFunction[Tuple2[String, String], (String, String, String, String),
      Tuple2[Tuple2[String, String], Int]]() {
      val map = collection.mutable.HashMap[String, Int]()

      override def processElement(i: (String, String, String, String), context: KeyedProcessFunction[(String, String), (String, String, String, String), ((String, String), Int)]#Context, collector: Collector[((String, String), Int)]): Unit = {
        val hashKey = i._3.concat("-").concat(i._2)
        var count = map.getOrElse(hashKey, 0)
        count= count+1
        collector.collect(context.getCurrentKey,count)
        map.update(hashKey,count)
      }
    })
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