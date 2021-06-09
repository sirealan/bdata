//package com.atguigu.hotItems
//
//import com.atguigu.bean.{ItemViewCount, UserBehavior}
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
//import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//import java.sql.Timestamp
//import scala.collection.mutable.ListBuffer
//
//class POJO {
//
//}
//// 聚合商品count值
//case class CountAgg() extends
//  org.apache.flink.api.common.functions.AggregateFunction[UserBehavior, Long, Long] {
//  override def add(in: UserBehavior, acc: Long): Long = acc + 1
//
//  override def createAccumulator(): Long = 0L
//
//  override def getResult(acc: Long): Long = acc
//
//  override def merge(acc: Long, acc1: Long): Long = acc + acc1
//}
//
//// 窗口聚合
//case class ItemViewWindowResult() extends
//  org.apache.flink.streaming.api.scala.function.WindowFunction[Long,
//    ItemViewCount, Tuple, TimeWindow] {
//
//  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//    out.collect(
//      ItemViewCount(itemId = key.asInstanceOf[Tuple1[Long]].f0,
//        windowEnd = window.getEnd, count = input.iterator.next
//      ))
//  }
//}
//
//case class TopNHotItems(topSize: Int) extends
//  KeyedProcessFunction[Tuple, ItemViewCount, String] {
//  var itemViewCountListState: ListState[ItemViewCount] = _
//
//  override def open(parameters: Configuration): Unit =
//    itemViewCountListState = getRuntimeContext.getListState(new
//        ListStateDescriptor[ItemViewCount]("itemViewCountList", classOf[ItemViewCount]))
//
//  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
//    itemViewCountListState.add(i) //每来一条就加入
//    context.timerService.registerEventTimeTimer(i.windowEnd + 1) // 注册windowEnd+1的定时器
//  }
//
//  // 所有窗口统计结果出来后，可以排序输出
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
//    val allItemViewCounts = ListBuffer[ItemViewCount]()
//    itemViewCountListState.get
//      .forEach(o => allItemViewCounts += o)
//    itemViewCountListState.clear()
//    var result = new StringBuffer(
//      s"窗口结束时间：${new Timestamp(timestamp - 1)}\n")
//    allItemViewCounts
//      .sortBy(_.count)(Ordering.Long.reverse) // 科里化
//      //      .reverse
//      .take(topSize)
//      .foreach(o => result.append(s"商品ID：${o.itemId}\t商品热度：${o.count}\n")
//        .append("=====================\n\n")
//      )
//    out.collect(result.toString)
//  }
//}