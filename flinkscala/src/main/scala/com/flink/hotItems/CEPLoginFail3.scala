package com.flink.hotItems

import com.flink.bean._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 恶意登录失败使用cep模式
 * 同一用户两秒内连续登录失败n次报警
 */

object CEPLoginFail3 extends App {

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
  // 一个登录事件以后，仅給另外一个登录事件 5秒内3次失败
  CEP.pattern(ds.keyBy(_.userId), Pattern
    .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
    .next("secondFail").where(_.eventType == "fail")
    .next("thirdFail").where(_.eventType == "fail")
    .within(Time.seconds(5))
  )
    .select(LoginFailEventMatch())
    .print("warning")

  env.execute
}

