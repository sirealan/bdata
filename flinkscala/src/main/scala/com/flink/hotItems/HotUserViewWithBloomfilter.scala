package com.flink.hotItems

import com.flink.bean.{MyTrigger, UVCountWithBloom, UserBehavior}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * .process(TotalPVCountResult())
 * 得出最终叠加的结果
 */
object HotUserViewWithBloomfilter extends App {

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
    .map(data => ("uv", data.userId))
    .keyBy(_._1)
    .timeWindow(Time.hours(1))
    // 触发器，每来一条数据直接出发窗口计算并清空状态
    .trigger(MyTrigger())
    .process(UVCountWithBloom())
    .print("window")

  env.execute
}

