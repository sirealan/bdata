package com.flink.hotItems

import com.flink.bean.MarketCountByChannel
import com.flink.util.SimulatedSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * .process(TotalPVCountResult())
 * 得出最终叠加的结果
 */

object AppMarketByChannel extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build)
  val ds =
    env
      //            .readTextFile("input/UserBehavior.csv")
      .addSource(SimulatedSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "uninstall")
      //      .keyBy("channel", "behavior")
      .keyBy(o => (o.channel, o.behavior))
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(MarketCountByChannel())
      .print("window")

  env.execute
}

