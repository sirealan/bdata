package com.flink.hotItems

import com.flink.bean._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 */

object CEPOrderTimeout extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val ds =
    env
      .readTextFile("input/OrderLog.csv")
      //      .addSource(SimulatedSource())
      .map(o => {
        val split = o.split(",")
        OrderEvent(split(0).toLong, split(1), split(2), split(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.orderId)
//      .print()
  val orderTimeout = new OutputTag[OrderResult]("orderTimeout")
  // 一个登录事件以后，仅給另外一个登录事件 5秒内3次失败
  val cep = CEP.pattern(ds, Pattern
    .begin[OrderEvent]("create").where(_.eventType == "create")
    .followedBy("pay").where(_.eventType == "pay")
    .within(Time.minutes(15))
  )
    .select(orderTimeout,
      OrderTimeoutSelect(),
      OrderPaySelect(),
    )
  cep
    .print("pay")
  cep.getSideOutput(orderTimeout).print("timeout")
  env.execute
}

