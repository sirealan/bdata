package com.flink.examples

import com.flink.bean.SensorReader
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types._

/**
 * 1.group window sql and table
 * 滚动时间窗口，每10秒钟统计一次
 * select
 * id,
 * count(id),
 * avg(temperature),
 * tumble_end(ts, interval '10' second)
 * from sensor
 * group by
 * id,
 * tumble(ts,interval '10' second)
 * 2.over window sql and table 统计前两条和目前这条数据的平均温度值
select
 id,
 ts,
 count(id) over ow,
 avg(temperature) over ow
from sensor
window ow as(
  partition by id
  order by ts
  rows between 2 preceding and current row
)
 */
object TableTimeAndWindow extends App {

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
  tenv.createTemporaryView("sensor", sensorTable)
  // 1.group window
  // 1.1  table api
  sensorTable.window(Tumble over 10.seconds on 'ts as 'tw) // 滚动时间窗口，每10秒钟统计一次，
    .groupBy('id, 'tw)
    .select('id, 'id.count, 'temperature.avg, 'tw.end) //id基数,温度平均,窗口结束
    .toAppendStream[Row]
  //    .print("table")
  // 1.2 sql api
  tenv.sqlQuery(
    """
      |select
      |id,
      |count(id),
      |avg(temperature),
      |tumble_end(ts, interval '10' second)
      |from sensor
      |group by
      |id,
      |tumble(ts, interval '10' second)
      """.stripMargin)
    .toRetractStream[Row]
  //      .print("sql")
  // 2 over window
  // 2.1 table
  sensorTable
    //    .window(Over.partitionBy('id).orderBy('ts).preceding(2.rows).as('ow))
    .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
    .select('id, 'id.count over 'ow, 'temperature.avg over 'ow)
    .toAppendStream[Row]
      .print("table")
  // 2.2 sql
      tenv.sqlQuery(
        """
          |select
          | id,
          | ts,
          | count(id) over ow,
          | avg(temperature) over ow
          |from sensor
          |window ow as (
          |  partition by id
          |  order by ts
          |  rows between 2 preceding and current row
          |)
          """.stripMargin)
        .toRetractStream[Row]
        .print("sql")
  //  sensorTable.printSchema
  //  sensorTable.toAppendStream[Row].print("row")
  env.execute
}
