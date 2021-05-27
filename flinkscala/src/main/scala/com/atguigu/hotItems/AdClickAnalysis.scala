package com.atguigu.hotItems

import com.atguigu.bean._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * .process(TotalPVCountResult())
 * 得出最终叠加的结果
 */

object AdClickAnalysis extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val ds =
    env
      .readTextFile("input/AdClickLog.csv")
      //      .addSource(SimulatedSource())
      .map(o => {
        val split = o.split(",")
        AdsClickLog(split(0).toLong, split(1).toLong, split(2),
          split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
  // 同一广告ID被同一用户点击超过n次算恶意刷单。
  val fliter = ds
    .keyBy(o => (o.userId, o.adId))
    .process(FilterBlackListUserResult(100))
  fliter.keyBy(o => o.province)
    .timeWindow(Time.hours(1), Time.seconds(5))
    .aggregate(AdCountAgg(), AdCountWindowResult())
    .print("window")
  fliter.getSideOutput(new OutputTag[BlackListUserWarning]("warning"))
    .print("warning")
  env.execute
}

