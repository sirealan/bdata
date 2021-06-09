package com.flink.hotItems

import com.flink.bean._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * 使用process处理，没有cep好
 */

object CEPOrderTimeout2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val orderTimeout = new OutputTag[OrderResult]("orderTimeout")
  val ds =
    env
      .readTextFile("input/OrderLog.csv")
      //      .addSource(SimulatedSource())
      .map(o => {
        val split = o.split(",")
        OrderEvent(split(0).toLong, split(1), split(2), split(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.orderId) // 一个登录事件以后，仅給另外一个登录事件 5秒内3次失败
      .process(OrderPayMatchResult(orderTimeout))
  ds
    .print("payed")
  ds.getSideOutput(orderTimeout).print("timeout")
  env.execute
}

