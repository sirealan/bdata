package com.flink.examples

import com.flink.bean.{OrderEvent, TxEvent}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProjectOrder extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //  val orders =
  env.readTextFile("input/OrderLog.csv")
    .flatMap(new FlatMapFunction[String,OrderEvent]() {
      override def flatMap(t: String, collector: Collector[OrderEvent]): Unit = {
        val split = t.split(",")
        split(1) match {
          case "pay" => collector.collect(OrderEvent(
            split(0).toLong,
            split(1),
            split(2),
            split(3).toLong
          ))
          case _ =>
        }
      }
    })
    .connect(
      env.readTextFile("input/ReceiptLog.csv")
        .map(o=>{
          val split = o.split(",")
          TxEvent(
            split(0),
            split(1),
            split(2).toLong
          )
        })
    )
    .keyBy("txId", "txId")
    .process(new KeyedCoProcessFunction[String,OrderEvent,TxEvent,(OrderEvent,TxEvent)] (){
      val tx = collection.mutable.HashMap.empty[String,TxEvent]
      val od = collection.mutable.HashMap.empty[String,OrderEvent]

      override def processElement1(in1: OrderEvent, context: KeyedCoProcessFunction[String, OrderEvent, TxEvent, (OrderEvent, TxEvent)]#Context, collector: Collector[(OrderEvent, TxEvent)]): Unit = {
        tx.contains(in1.txId) match {
          case true => collector.collect(in1,tx.get(in1.txId).get)
          case false => od.update(in1.txId, in1)
        }
      }

      override def processElement2(in2: TxEvent, context: KeyedCoProcessFunction[String, OrderEvent, TxEvent, (OrderEvent, TxEvent)]#Context, collector: Collector[(OrderEvent, TxEvent)]): Unit = {
        od.contains(in2.txId) match {
          case true => collector.collect(od.get(in2.txId).get,in2)
          case false => tx.update(in2.txId, in2)
        }
      }
    })

    .print()
  env.execute()
}
