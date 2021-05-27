package com.atguigu.hotItems

import com.atguigu.bean.{PvCountAgg, PvCountWindowResult, TotalPVCountResult, UserBehavior}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * .process(TotalPVCountResult())
 * 得出最终叠加的结果
 */
object HotPageView2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build)
  val ds =
    env
      .readTextFile("input/UserBehavior.csv")
      //    .addSource(KfUtil.source("hotitems"))
      .map(o => {
        val split = o.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt,
          split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 指定timestamp生成watermark
  ds
    .filter(_.behavior == "pv")
        .map(data => ("pv", 1L))
//    .map(d => (Random.nextString(10), 1L))
//        .map(MyMapper())
    .keyBy(_._1)
    .timeWindow(Time.hours(1))
    .aggregate(PvCountAgg(), PvCountWindowResult())
    .keyBy(_.windowEnd)
//    .sum(1)
//    .sum("count")
    .process(TotalPVCountResult())
    .print("window")

  env.execute
}

