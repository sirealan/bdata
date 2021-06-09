package com.flink.examples

import com.flink.bean.SensorReader
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutput extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  env.setStateBackend(new RocksDBStateBackend())
  val ds = env.socketTextStream("localhost", 9999)
    .map(o => {
      val split = o.split(",")
      SensorReader(
        split(0),
        split(1).toLong,
        split(2).toDouble
      )
    })
    //    .process(new SplitTempProcessor(30.0))
    .process(new SplitTempProcessor)

  ds.print("high")
  ds.getSideOutput(new OutputTag[SensorReader]("general")).print("general")
  ds.getSideOutput(new OutputTag[SensorReader]("low")).print("low")

  env.execute
}

// SplitTempProcessor
class SplitTempProcessor extends ProcessFunction[SensorReader, SensorReader] {
  override def processElement(i: SensorReader, context: ProcessFunction[SensorReader, SensorReader]#Context, collector: Collector[SensorReader]): Unit = {
    if (i.temperature > 40) collector.collect(i)
    else if (i.temperature > 20 && i.temperature <= 40) context.output(new OutputTag[SensorReader]("general"), i)
    else context.output(new OutputTag[SensorReader]("low"), i)
  }
}