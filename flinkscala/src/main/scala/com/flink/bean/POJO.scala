package com.flink.bean

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.java.tuple._
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.sql.Timestamp
import java.util
import scala.collection.mutable.ListBuffer
import scala.util.Random


case class WaterSensor(id: String, ts: Int, vc: String)

case class SensorReader(id: String, ts: Long, temperature: Double)

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int,
                        behavior: String, timestamp: Long)

case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

case class ApacheLog(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)

case class PVCount(windowEnd: Long, count: Long)

case class UVCount(windowEnd: Long, count: Long)

case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

case class AdsClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdsClickCountByProvince(windowEnd: String, province: String, count: Long)

case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

case class TxEvent(txId: String, payChannel: String, timestamp: Long)

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, msg: String)

case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

case class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String) = s.hashCode * factor - 10000
}

case class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(s: String): Unit = {
    s.split(separator).foreach(o => collect((o, o.length)))
  }
}

case class AvgTempAcc(var sum: Double = 0, var count: Int = 0)

// ?????????????????????avg???(tempSum,tempCount)
case class AvgTemp() extends AggregateFunction[Double, AvgTempAcc] {
  //class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count

  override def createAccumulator(): AvgTempAcc = AvgTempAcc() //new

  def accumulate(accumulator: AvgTempAcc, temp: Double): Unit = {
    accumulator.sum += temp
    accumulator.count += 1
  }
}

case class TopTemp(var highest: Double = Double.MinValue,
                   var secondHigh: Double = Double.MinValue)

case class TopN() extends org.apache.flink.table.functions.TableAggregateFunction[(Double, Int), TopTemp] {
  override def createAccumulator(): TopTemp = TopTemp() //new

  def accumulate(acc: TopTemp, temp: Double): Unit = {
    if (temp > acc.highest) {
      acc.secondHigh = acc.highest
      acc.highest = temp
    } else if (temp > acc.secondHigh) acc.secondHigh = temp
  }

  // ????????????????????????????????????
  def emitValue(acc: TopTemp, out: Collector[(Double, Int)]): Unit = {
    out.collect((acc.highest, 1))
    out.collect((acc.secondHigh, 2))
  }
}

// ????????????count???
case class CountAgg() extends
  org.apache.flink.api.common.functions.AggregateFunction[UserBehavior, Long, Long] {
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// ????????????
case class ItemViewWindowResult() extends
  org.apache.flink.streaming.api.scala.function.WindowFunction[Long,
    ItemViewCount, Tuple, TimeWindow] {

  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(
      ItemViewCount(itemId = key.asInstanceOf[Tuple1[Long]].f0,
        windowEnd = window.getEnd, count = input.iterator.next
      ))
  }
}

case class TopNHotItems(topSize: Int) extends
  KeyedProcessFunction[Tuple, ItemViewCount, String] {
  var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit =
    itemViewCountListState = getRuntimeContext.getListState(new
        ListStateDescriptor[ItemViewCount]("itemViewCountList", classOf[ItemViewCount]))

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemViewCountListState.add(i) //?????????????????????
    context.timerService.registerEventTimeTimer(i.windowEnd + 1) // ??????windowEnd+1????????????
  }

  // ??????????????????????????????????????????????????????
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItemViewCounts = ListBuffer[ItemViewCount]()
    itemViewCountListState.get
      .forEach(o => allItemViewCounts += o)
    itemViewCountListState.clear()
    val result = new StringBuffer(
      s"?????????????????????${new Timestamp(timestamp - 1)}\n")
    allItemViewCounts
      .sortBy(_.count)(Ordering.Long.reverse) // ?????????
      //      .reverse
      .take(topSize)
      .foreach(o => result.append(s"??????ID=${o.itemId}\t????????????=${o.count}\n")
        .append("=====================\n\n")
      )
    out.collect(result.toString)
  }
}

case class PageCountAgg()
  extends org.apache.flink.api.common.functions.AggregateFunction[ApacheLog, Long, Long] {
  override def add(in: ApacheLog, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

case class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next))
  }
}

case class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //  lazy val pageViewCountList = getRuntimeContext.getListState(
  //    new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
  lazy val pageViewCountMap = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))


  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    pageViewCountMap.put(i.url, i.count)
    context.timerService.registerEventTimeTimer(i.windowEnd + 1)
    // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????
    context.timerService.registerEventTimeTimer(i.windowEnd + 60000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allPageViewCount = ListBuffer[(String, Long)]()
    //    pageViewCountList.get.forEach(o => {
    //      allPageViewCount += o
    //    })

    pageViewCountMap.entries.forEach(o => allPageViewCount += ((o.getKey, o.getValue)))
    //    pageViewCountList.clear
    if (timestamp == ctx.getCurrentKey + 60000) {
      pageViewCountMap.clear // ?????????????????????????????????????????????
      return
    }
    val result = new StringBuffer(
      s"?????????????????????${new Timestamp(timestamp - 1)}\n")
    allPageViewCount
      //      .sortBy(_.count)(Ordering.Long.reverse) // ?????????
      .sortWith(_._2 > _._2)
      //      .reverse
      .take(n)
      .foreach(o => result.append(s"url=${o._1}\t??????=${o._2}\n")
        .append("=====================\n\n")
      )
    out.collect(result.toString)
  }
}

case class PvCountAgg()
  extends org.apache.flink.api.common.functions.AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

case class PvCountWindowResult() extends WindowFunction[Long, PVCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PVCount]): Unit = {
    out.collect(PVCount(window.getEnd, input.head))
  }
}

// ????????????key
case class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
  override def map(t: UserBehavior): (String, Long) = (Random.nextString(10), 1L)
}

case class TotalPVCountResult() extends KeyedProcessFunction[Long, PVCount, PVCount] {
  // ??????count??????
  lazy val totalPv = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("pv-count", classOf[Long]))

  override def processElement(i: PVCount, context: KeyedProcessFunction[Long, PVCount, PVCount]#Context, collector: Collector[PVCount]): Unit = {
    totalPv.update(totalPv.value + i.count)
    context.timerService.registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PVCount, PVCount]#OnTimerContext, out: Collector[PVCount]): Unit = {
    out.collect(PVCount(ctx.getCurrentKey, totalPv.value))
    totalPv.clear
  }
}

case class UVCountResult() extends AllWindowFunction[UserBehavior, UVCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
    //    var uIdSet = Set[Long]()
    //    input.foreach(u => uIdSet += u.userId)
    //    out.collect(UVCount(window.getEnd, uIdSet.size))
    out.collect(UVCount(window.getEnd, input.map(_.userId).toSet[Long].size))
  }
}

case class MyTrigger() extends Trigger[(String, Long), TimeWindow]() {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
    TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

/*
?????????hash??????
 */
case class Bloom(size: Long) extends Serializable {
  private val cap = size // ??????cap???2????????????

  // hash??????
  def hash(value: String, seed: Int): Long = {
    var result = 0
    (0 until value.length).foreach(i => result = result * seed + value.charAt(i))
    // ??????hash???????????????cap?????????
    (cap - 1) & result
  }
}

case class UVCountWithBloom() extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {
  lazy val jedis = new Jedis("localhost", 6379)
  // 64MB(bit) ???????????? 2^6(64) * 2^20(1m) * 2^3(8bit)
  // 1<<29 eq 2^28
  lazy val bloomFilter = Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVCount]): Unit = {
    // redis????????????key
    val storedBitMapKey = context.window.getEnd.toString
    // ??????????????????uv count????????????????????????Redis????????????uvcount???hash??????(windowEnd,count)
    val uvCountMap = "uvcount"
    val currentKey = context.window.getEnd.toString
    var count = 0L
    if (jedis.hget(uvCountMap, currentKey) != null)
      count = jedis.hget(uvCountMap, currentKey).toLong
    // ?????????????????????userId?????????hash????????????????????????0
    val userId = elements.last._2.toString
    // ??????hash?????????????????????????????????
    val offset = bloomFilter.hash(userId, 61)
    // ??????redis??????bitmap????????????
    val isExist = jedis.getbit(storedBitMapKey, offset)
    // ???????????????????????????1 true
    if (!isExist) {
      jedis.setbit(storedBitMapKey, offset, true)
      jedis.hset(uvCountMap, currentKey, (count + 1).toString)
    }
  }
}

case class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    out.collect(MarketViewCount(new Timestamp(context.window.getStart).toString,
      new Timestamp(context.window.getEnd).toString,
      key._1, key._2, elements.size))
  }
}

case class AdCountAgg() extends
  org.apache.flink.api.common.functions.AggregateFunction[AdsClickLog, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(in: AdsClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
 * in Long ???AdCountAgg()??????????????????
 */
case class AdCountWindowResult() extends WindowFunction[Long, AdsClickCountByProvince, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdsClickCountByProvince]): Unit = {
    out.collect(AdsClickCountByProvince(new Timestamp(window.getEnd).toString,
      key, input.head))

  }
}

case class FilterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdsClickLog, AdsClickLog] {
  lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
  lazy val isBlackState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

  override def processElement(i: AdsClickLog, context: KeyedProcessFunction[(Long, Long), AdsClickLog, AdsClickLog]#Context, collector: Collector[AdsClickLog]): Unit = {
    val curcount = countState.value
    // ?????????????????????????????????????????????0???????????????????????????
    if (curcount == 0) {
      val ts = (context.timerService.currentProcessingTime /
        (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
      resetState.update(ts)
      context.timerService.registerProcessingTimeTimer(ts)
    }
    // ??????????????????????????????
    if (curcount >= maxCount) {
      // ??????????????????????????????????????????????????????????????????
      if (!isBlackState.value) {
        isBlackState.update(true)
        context.output(new OutputTag[BlackListUserWarning]("warning"),
          BlackListUserWarning(i.userId, i.adId, s"click ad more than $maxCount today"))
      }
      return
    }
    // ???????????????count+1???????????????????????????
    countState.update(curcount + 1)
    collector.collect(i)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdsClickLog, AdsClickLog]#OnTimerContext, out: Collector[AdsClickLog]): Unit = {
    if (timestamp == resetState) {
      isBlackState.clear
      countState.clear
    }
  }
}

/**
 * ???????????????????????????????????????n?????????
 *
 * @param failTime
 */
case class LoginFailWarningResult(failTime: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  lazy val loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
  lazy val tsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    if (i.eventType == "fail") {
      loginFailListState.add(i)
      if (tsState.value == 0) { //????????????????????????????????????2?????????
        val ts = i.timestamp * 1000 + 2000
        context.timerService.registerEventTimeTimer(ts)
        tsState.update(ts)
      }
    } else {
      // ???????????????????????????????????????????????????
      context.timerService.deleteEventTimeTimer(tsState.value)
      loginFailListState.clear
      tsState.clear
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {

    val allLoginFailList = ListBuffer[LoginEvent]()
    loginFailListState.get.forEach(o => allLoginFailList += o)
    if (allLoginFailList.length > failTime) {
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          s"login fail in 2s for ${allLoginFailList.length} times"
        ))
    }
    // ????????????
    loginFailListState.clear
    tsState.clear
  }
}

/**
 * ???????????????????????????????????????n?????????
 *
 * @param failTime
 */
case class LoginFailWarningAdvanceResult(failTime: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  lazy val loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
  //  lazy val tsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    if (i.eventType == "fail") {
      val iter = loginFailListState.get.iterator
      // ???????????????????????????????????????
      if (iter.hasNext) {
        val current = iter.next
        // ????????????????????????????????????
        if (i.timestamp < current.timestamp + 2) {
          collector.collect(
            LoginFailWarning(
              i.userId,
              current.timestamp,
              i.timestamp,
              s"login fail in 2s for times"
            ))
        }
        // ?????????????????????????????????????????????????????????????????????
        loginFailListState.clear
        loginFailListState.add(i)
      } else {
        // ???????????????????????????liststate
        loginFailListState.add(i)
      }
    } else {
      // ?????????????????????????????????
      loginFailListState.clear
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {

    val allLoginFailList = ListBuffer[LoginEvent]()
    loginFailListState.get.forEach(o => allLoginFailList += o)
    if (allLoginFailList.length > failTime) {
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          s"login fail in 2s for ${allLoginFailList.length} times"
        ))
    }
    // ????????????
    loginFailListState.clear
  }
}

case class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    LoginFailWarning(
      map.get("firstFail").get(0).userId,
      map.get("firstFail").get(0).timestamp,
      //      map.get("secondFail").iterator.next.timestamp,
      map.get("thirdFail").iterator.next.timestamp,
      //      map.get("secondFail").get(0).timestamp,
      "login fail"
    )
  }
}

case class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    OrderResult(
      map.get("create").iterator().next().orderId,
      s"timeout : $l"
    )
  }
}

case class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {

  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    OrderResult(
      map.get("pay").iterator().next().orderId,
      s"pay success"
    )
  }
}

case class OrderPayMatchResult(orderTimeout: OutputTag[OrderResult]) extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  lazy val iscreateState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("iscreate", classOf[Boolean]))
  lazy val ispayState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispay", classOf[Boolean]))
  lazy val tsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    val iscreate = iscreateState.value()
    val ispay = ispayState.value()
    val ts = tsState.value()
    if (i.eventType == "create") {
      if (ispay) { // ??????????????????????????????????????????
        collector.collect(OrderResult(i.orderId, "pay successfully"))
        iscreateState.clear()
        ispayState.clear()
        tsState.clear()
        context.timerService().deleteEventTimeTimer(ts)
      } else {
        // ??????????????????????????????????????????15????????????????????????
        val ts = i.timestamp * 1000 + 15 * 60 * 1000L
        context.timerService.registerEventTimeTimer(ts)
        tsState.update(ts)
        ispayState.update(true)
      }
    } else if (i.eventType == "pay") { //?????????????????????????????????crete??????????????????
      if (iscreate) { //???????????????????????????????????????????????????
        if (i.timestamp * 1000 < ts) collector.collect(OrderResult(i.orderId, "pay successfully"))
        else context.output(orderTimeout, OrderResult(i.orderId, "payed but already timeout"))
        // ??????????????????????????????
        iscreateState.clear()
        ispayState.clear()
        tsState.clear()
        context.timerService().deleteEventTimeTimer(ts)
      } else {
        // ??????create?????????????????????????????????????????????
        context.timerService().registerEventTimeTimer(i.timestamp * 1000)
        tsState.update(i.timestamp * 1000)
        ispayState.update(true)
      }

    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //??????pay??????????????????create
    if (ispayState.value) ctx.output(orderTimeout, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
    else ctx.output(orderTimeout, OrderResult(ctx.getCurrentKey, "pay timeout"))
    //????????????
    iscreateState.clear()
    ispayState.clear()
    tsState.clear()
  }
}

case class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  lazy val orderState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("orderstate", classOf[OrderEvent]))
  lazy val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptstate", classOf[ReceiptEvent]))

  val unmatchOrderOpt = new OutputTag[OrderEvent]("umatch-order")
  val unmatchReceiptOpt = new OutputTag[ReceiptEvent]("umatch-receipt")

  override def processElement1(in1: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt = receiptState.value()
    if (receipt != null) {
      collector.collect((in1, receipt))
      orderState.clear()
      receiptState.clear()
    } else {
      // ??????????????????
      context.timerService().registerEventTimeTimer(in1.timestamp * 1000 + 5 * 1000)
      orderState.update(in1)
    }
  }

  override def processElement2(in2: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val order = orderState.value()
    if (order != null) {
      collector.collect((order, in2))
      orderState.clear()
      receiptState.clear()
    } else {
      // ????????????3???
      context.timerService().registerEventTimeTimer(in2.timestamp * 1000 + 3 * 1000)
      receiptState.update(in2)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // ???????????????,???????????????????????????????????????????????????
    if (orderState.value() != null) ctx.output(unmatchOrderOpt, orderState.value())
    if (receiptState.value() != null) ctx.output(unmatchReceiptOpt, receiptState.value())
  }
}

case class TxMatchWithJoinResult() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((in1, in2))
  }
}
