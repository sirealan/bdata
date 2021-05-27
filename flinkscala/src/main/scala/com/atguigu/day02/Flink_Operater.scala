package com.atguigu.day02

import org.apache.flink.streaming.api.scala._


object Flink_Operater extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //  env.setParallelism(1)
  //  env.fromElements(10, 3, 5, 9, 20, 8)
  //  val a = env.fromElements(10, 3, 5)
  //  val b = env.fromElements("a","b","c")
  //  val a = env.fromElements(1, 2, 3, 4, 5)
  //  val b = env.fromElements(10, 20, 30, 40, 50)
  //  val c = env.fromElements(100, 200, 300, 400, 500)
  //
  //    .keyBy(_ % 2 == 0)
  //    .shuffle
  //    a.union(b).union(c)
  //    .sum(0)
  //    .max(0)
  env.fromCollection(
    List(
      WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 50),
      WaterSensor("sensor_1", 1607527996000L, 30),
      WaterSensor("sensor_2", 1607527993000L, 10),
      WaterSensor("sensor_2", 1607527995000L, 30)
    )
  )
//    .keyBy(_.id)
    //    .reduce((a,b)=>WaterSensor(a.id,a.ts,a.vc+b.vc))
//    .process(new ProcessFunction[WaterSensor, Tuple2[String, Long]]() {
//      override def processElement(value: WaterSensor, ctx: Context,
//                                  out: Collector[Tuple2]): Unit = {
//
//        out.collect(Tuple2(value.id, value.vc))
//      }
//    }
//    )
    .print

  env.execute()


}

//@JsonCodec
case class WaterSensor(id: String, ts: Long, vc: Long)
