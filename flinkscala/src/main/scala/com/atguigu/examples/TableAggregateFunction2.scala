package com.atguigu.examples

import com.atguigu.bean.{SensorReader, TopN}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types._

/**
 * topN
 */
object TableAggregateFunction2 extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val tenv = StreamTableEnvironment.create(env,
    EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build)
  val ds = env.readTextFile("input/sensor.txt")
    .map(o => {
      val split = o.split(",")
      SensorReader(
        split(0),
        split(1).toLong,
        split(2).toDouble
      )
    })
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReader](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReader): Long = t.ts * 1000
    })
  val sensorTable = tenv.fromDataStream(ds, 'id, 'temperature, 'ts.rowtime)
  val topN = TopN()
  tenv.registerFunction("topN", topN)
  tenv.createTemporaryView("sensor", sensorTable)
  // 1.1  table api
  sensorTable
    .groupBy('id)
    .flatAggregate(topN('temperature) as ('temp, 'rank))
    .select('id, 'temp, 'rank)
    .toRetractStream[Row] // 注意这里是toRetractStream不是toAppendStream
    .print("table")
  // 1.2 sql api
//  tenv.sqlQuery(
//    "select id,avgTemp(temperature) from sensor group by id")
//    .toRetractStream[Row]
//    .print("sql")

  //  sensorTable.printSchema
  //  sensorTable.toAppendStream[Row].print("row")
  env.execute
}