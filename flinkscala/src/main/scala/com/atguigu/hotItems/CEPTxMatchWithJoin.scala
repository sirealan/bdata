package com.atguigu.hotItems

import com.atguigu.bean._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 */

object CEPTxMatchWithJoin extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val orderTimeout = new OutputTag[OrderResult]("orderTimeout")
  val unmatchOrderOpt = new OutputTag[OrderEvent]("umatch-order")
  val unmatchReceiptOpt = new OutputTag[ReceiptEvent]("umatch-receipt")

  val ds =
    env
      .readTextFile("input/OrderLog.csv")
      .map(o => {
        val split = o.split(",")
        OrderEvent(split(0).toLong, split(1), split(2), split(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.eventType == "pay")
      .keyBy(_.txId)
      .intervalJoin(
        env
          .readTextFile("input/ReceiptLog.csv")
          .map(o => {
            val split = o.split(",")
            ReceiptEvent(split(0), split(1), split(2).toLong)
          })
          .assignAscendingTimestamps(_.timestamp * 1000)
          .keyBy(_.txId))
      .between(Time.seconds(-3), Time.seconds(5))
      .process(TxMatchWithJoinResult())
  //  ds
  ds.print("total")
  env.execute
}

