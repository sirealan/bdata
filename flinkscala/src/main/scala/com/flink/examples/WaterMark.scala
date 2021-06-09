package com.flink.examples

import com.flink.bean.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

object WaterMark extends App {
  val msm =
    WatermarkStrategy
      .forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(3L))
      .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(t: WaterSensor, l: Long): Long = t.ts * 1000
      })
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.socketTextStream("localhost", 9999)
    .map(m => {
      val split = m.split(",")
      WaterSensor(
        split(0),
        split(1).toInt,
        split(2)
      )
    })
    .assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(3L))
      .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(t: WaterSensor, l: Long): Long = t.ts*1000
      })
    )
    .keyBy(_.id)
    .window(TumblingEventTimeWindows
      .of(Time.seconds(3))
//      .assignWindows()
    )
    .process(new ProcessWindowFunction[WaterSensor,String,String,TimeWindow]() {
      override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
        out.collect(s"当前key: $key"
          .concat(s"窗口: [${context.window.getStart / 1000},${context.window.getEnd / 1000}) 一共有 ")
          .concat(s"${elements.size}条数据"))
      }
    })
//    .assignTimestampsAndWatermarks(
//      WatermarkStrategy
//        .forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(3L))
//        .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
//          override def extractTimestamp(t: WaterSensor, l: Long): Long = t.ts * 1000
//        })
//    )
//    .keyBy(_.id)
//    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//    .process(new ProcessWindowFunction[WaterSensor, String, String, TimeWindow]() {
//      override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
//        out.collect(s"当前key: $key"
//          .concat(s"窗口: [${context.window.getStart / 1000},${context.window.getEnd / 1000}) 一共有 ")
//          .concat(s"${elements.size}条数据"))
//      }
//    })
    .print
//    .addSink()

  env.execute
}
