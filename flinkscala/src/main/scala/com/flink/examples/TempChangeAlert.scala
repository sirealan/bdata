package com.flink.examples

import com.flink.bean.SensorReader
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempChangeAlert extends App {

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
    //    .flatMap(new AlertFlatMapFunc(10.0))
    .flatMapWithState[(String, Double, Double), Double]({
      case (data: SensorReader, None) => (List.empty, Some(data.temperature))
      case (data: SensorReader, lastTemp: Some[Double]) =>
        if ((lastTemp.get - data.temperature).abs > 10.0)
          (List((data.id, lastTemp.value, data.temperature)), Some(data.temperature))
        else
          (List.empty, Some(data.temperature))

    })
    .print
  env.execute
}

class AlertFlatMapFunc(d: Double) extends RichFlatMapFunction[SensorReader, (String, Double, Double)] {
  lazy val lasttempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lasttemp", classOf[Double]))

  override def flatMap(in: SensorReader, collector: Collector[(String, Double, Double)]): Unit = {
    val lasttemp = lasttempState.value()
    //    println("更新完成1："+lasttempState.value())
    if (((in.temperature - lasttemp).abs) > d)
      collector.collect((in.id, lasttemp, in.temperature))
    lasttempState.update(in.temperature)
  }
}