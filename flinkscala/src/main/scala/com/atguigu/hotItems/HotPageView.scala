package com.atguigu.hotItems

import com.atguigu.bean.{ApacheLog, PageCountAgg, PageViewCountWindowResult, TopNHotPages}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

import java.text.SimpleDateFormat

object HotPageView extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build)
  val ds =
    env
      .readTextFile("input/apache.log")
      //    .addSource(KfUtil.source("hotitems"))
      .map(o => {
        val split = o.split(" ")
        ApacheLog(split(0), split(1),
          new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(split(3)).getTime
          , split(5), split(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
        override def extractTimestamp(t: ApacheLog): Long = t.timestamp //注意这里*不*1000取决是是毫秒还是秒，秒就要*
      })
  val aggTable =
    ds.filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLog]("late"))
      .aggregate(PageCountAgg(), PageViewCountWindowResult())
  aggTable.keyBy(_.windowEnd)
    .process(TopNHotPages(3))
    .print("sql")
  aggTable
    .getSideOutput(new OutputTag[ApacheLog]("late"))
    .print("late")
  env.execute
}

