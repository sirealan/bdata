package com.atguigu.hotItems

import com.atguigu.bean.{UVCount, UserBehavior}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.util.Collector

/**
 * .process(TotalPVCountResult())
 * 得出最终叠加的结果
 */
object HotUserView extends App {

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
    .timeWindowAll(Time.hours(1)) // 直接不分组、基于datastream一小时滚动。如果是先keyby，后面才是window
//    .apply(UVCountResult())
    .apply(new AllWindowFunction[UserBehavior, UVCount, TimeWindow](){
      override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
        out.collect(UVCount(window.getEnd, input.map(_.userId).toSet.size))
      }
    })
    .print("window")

  env.execute
}

