package com.scala.streaming

import com.scala.util.BaseUtil
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.streaming.Seconds

object BaseExample {

  def wc {
    val sc = BaseUtil.baseStreamingSc()
    sc
      .socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()
    // 1. 启动采集器
    sc.start()
    // 2. 等待采集器的关闭
    sc.awaitTermination()
  }

  /**
   * ˙注意状态处理必须加上checkpoint
   */
  def state {
    val sc = BaseUtil.baseStreamingSc()
    sc.checkpoint("cp")
    sc
      .socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      //      .reduceByKey(_+_)
      // updateStateByKey：根据key对数据的状态进行更新
      // 传递的参数中含有两个值
      // 第一个值表示相同的key的value数据
      // 第二个值表示缓存区相同key的value数据
      .updateStateByKey((seq: Seq[Int], os: Option[Int]) => Option(os.getOrElse(0) + seq.sum))
      .print()
    // 1. 启动采集器
    sc.start()
    // 2. 等待采集器的关闭
    sc.awaitTermination()
  }

  /**
   * ˙注意状态处理必须加上checkpoint
   */
  def stateJoin {
    val sc = BaseUtil.baseStreamingSc()
    sc.checkpoint("cp")
    sc
      .socketTextStream("localhost", 9999)
      .map((_, 9))
      .join(
        sc
          .socketTextStream("localhost", 8888)
          .map((_, 8))
      )
      .print()
    sc.start()
    sc.awaitTermination()
  }

  /**
   * ˙注意状态处理必须加上checkpoint
   */
  def stateWindow {
    val sc = BaseUtil.baseStreamingSc()
    sc.checkpoint("cp")
    sc
      .socketTextStream("localhost", 9999)
      .map((_, 9))
      // 窗口的范围应该是采集周期的整数倍
      // 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
      // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
      .window(Seconds(6), Seconds(6))
      .reduceByKey(_ + _)
      .print()
    sc.start()
    sc.awaitTermination()
  }

  /**
   * ˙注意状态处理必须加上checkpoint
   */
  def stateWindow1 {
    val sc = BaseUtil.baseStreamingSc()
    //    sc.checkpoint("cp")
    sc
      .socketTextStream("localhost", 9999)
      .map((_, 1))
      // reduceByKeyAndWindow : 当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
      // 无需重复计算，提升性能。
      .reduceByKeyAndWindow(
        (x, y) => x + y,
        (x, y) => x - y,
        Seconds(9),
        Seconds(3))
      .reduceByKey(_ + _)
      .print()
    sc.start()
    sc.awaitTermination()
  }

}