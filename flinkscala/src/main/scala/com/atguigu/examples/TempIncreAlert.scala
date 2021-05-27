package com.atguigu.examples

import com.atguigu.bean.SensorReader
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreAlert extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.socketTextStream("localhost", 9999)
    .map(o => {
      val split = o.split(",")
      SensorReader(
        split(0),
        split(1).toLong,
        split(2).toDouble
      )
    })
    .keyBy(_.id)
    .process(new IncreAlert(10000L))
    .print
  env.execute
}

class IncreAlert(intervel: Long) extends KeyedProcessFunction[String, SensorReader, String] {
  lazy val lasttempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lasttemp", classOf[Double]))
  lazy val timertsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerts", classOf[Long]))

  override def processElement(i: SensorReader, context: KeyedProcessFunction[String, SensorReader, String]#Context, collector: Collector[String]): Unit = {
    val lasttemp = lasttempState.value
    val timerts = timertsState.value
    lasttempState.update(i.temperature)
    if (i.temperature > lasttemp && timerts == 0) {
      val ts = context.timerService.currentProcessingTime + intervel
      context.timerService.registerProcessingTimeTimer(ts)
      timertsState.update(ts)
    } else if (i.temperature < lasttemp) {
      context.timerService.deleteProcessingTimeTimer(timerts)
      timertsState.clear
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReader, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(s"传感器：${ctx.getCurrentKey}连续${intervel / 1000}秒温度上升 ")
    timertsState.clear()
  }
}