package com.atguigu.hotItems

import com.atguigu.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * kafka-console-producer.sh --broker-list hd1:9092,hd2:9092,hd3:9092 --topic hotitems
 * sql 开窗api
 * select * from (
 * select
 * ,
 * row_number() over(partition by windowEnd order by cnt desc) as row_num
 * from aggtable
 * ) where row_num <=5
 * // pure sql impl
 * select * from (
 * select
 * ,
 * row_number() over(partition by itemId order by cnt desc) as row_num
 * from (
 * select
 * itemId,
 * hop_end(ts, interval '5' minute, interval '1' hour)
 * from datatable
 * where behavior='pv'
 * group by itemId,hop(ts, interval '5' minute, interval '1' hour)
 * )
 * ) where row_num <= 5
 */
object HotItemWithSql extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val tenv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build)
  val ds =
    env
      .readTextFile("input/UserBehavior.csv")
      //    .addSource(KfUtil.source("hotitems"))
      .map(o => {
        val split = o.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt,
          split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 指定timestamp生成watermark
  val dataTable = tenv
    .fromDataStream(ds, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
  // prepare for the pure sql api
  tenv.createTemporaryView("datatable", ds, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
  // table api
  val aggTable =
    dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)
  tenv.createTemporaryView("aggtable", aggTable, 'itemId, 'windowEnd, 'cnt)
  // sql api
  tenv.sqlQuery(
    """
      |select * from (
      |    select
      |    *,
      |    row_number() over(partition by windowEnd order by cnt desc) as row_num
      |    from aggtable
      |) where row_num <=5
      """.stripMargin)
    .toRetractStream[Row]
  //    .print("sql")
  // pure sql impl
  tenv.sqlQuery(
    """
      |select * from (
      |    select
      |    *,
      |    row_number() over(partition by itemId order by cnt desc) row_num
      |    from (
      |        select
      |        itemId,
      |        hop_end(ts, interval '5' minute, interval '1' hour),
      |        count(itemId) cnt
      |        from datatable
      |        where behavior='pv'
      |        group by itemId,hop(ts, interval '5' minute, interval '1' hour)
      |    )
      |) where row_num <= 5
      """.stripMargin)
    .toRetractStream[Row]
    .print("pure sql")
  env.execute
}