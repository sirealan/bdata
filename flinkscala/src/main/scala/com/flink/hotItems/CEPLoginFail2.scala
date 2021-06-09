package com.flink.hotItems

import com.flink.bean._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 恶意登录失败
 * 同一用户两秒内连续登录失败n次报警
 */

object CEPLoginFail2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val ds =
    env
      .readTextFile("input/LoginLog.csv")
      //      .addSource(SimulatedSource())
      .map(o => {
        val split = o.split(",")
        LoginEvent(split(0).toLong, split(1), split(2), split(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: LoginEvent): Long = t.timestamp * 1000
      })
      // 同一用户两秒内连续登录失败n次报警
      .keyBy(_.userId)
      .process(LoginFailWarningAdvanceResult(2))
      .print("warning")
  env.execute
}

