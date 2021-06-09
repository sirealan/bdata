package com.flink.hotItems

import com.flink.bean.{CountAgg, ItemViewWindowResult, TopNHotItems, UserBehavior}
import com.flink.util.KfUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.scala.

/**
 * kafka-console-producer.sh --broker-list hd1:9092,hd2:9092,hd3:9092 --topic hotitems
 */
object HotItem extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  //  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build)
  //  val ds =
  env
    //      .readTextFile("input/UserBehavior.csv")
    .addSource(KfUtil.source("hotitems"))
    .map(o => {
      val split = o.split(",")
      UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt,
        split(3), split(4).toLong)
    })
    .assignAscendingTimestamps(_.timestamp * 1000L) // 指定timestamp生成watermark
    .filter(_.behavior.equals("pv"))
    .keyBy("itemId")
    .timeWindow(Time.hours(1), Time.minutes(5))
    .aggregate(CountAgg(), ItemViewWindowResult())
    .keyBy("windowEnd") //窗口分组
    .process(TopNHotItems(5))
    .print()
  env.execute
}