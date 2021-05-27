package com.atguigu.examples

import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TumblingWin extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.socketTextStream("localhost", 9999)
    .flatMap(new FlatMapFunction[String, (String, Long)] {
      override def flatMap(t: String, collector: Collector[(String, Long)]): Unit = {
        t.split("\\W+")
          .foreach(collector.collect(_, 1))
      }
    })
    .keyBy(_._1)
    //    .window(TumblingProcessingTimeWindows.of(Time.of(10, TimeUnit.SECONDS)))
    //    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
    //    .reduce(new ReduceFunction[(String, Long)] {
    //      override def reduce(t: (String, Long), t1: (String, Long)): (String, Long) = {
    //        println(t+"-"+t1)
    //        (t._1,t._2+t1._2)
    //      }
    //    })
    //    .aggregate(new AggregateFunction[(String, Long), Long, Long]() {
    //      override def createAccumulator(): Long = 0L
    //
    //      override def add(in: (String, Long), acc: Long): Long = in._2 + acc
    //
    //      override def getResult(acc: Long): Long = acc
    //
    //      override def merge(acc: Long, acc1: Long): Long = acc + acc1
    //    })
    .process(new ProcessWindowFunction[(String, Long),
      (String, Long), String, TimeWindow]() {
      override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
        println(context.window.getStart)
        var sum = 0L;
        elements.foreach(u=>{sum=sum+u._2})
        out.collect((key, sum));

      }
    })

    //    .sum(1)
    .print()
  env.execute()
}

class AverageAggregate extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator() = 0L

  override def add(in: (String, Long), acc: Long): Long = {
    acc + in._2
  }

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}