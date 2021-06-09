package com.flink.util

import cn.hutool.core.thread.ThreadUtil
import com.flink.bean.MarketUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.util.UUID
import scala.util.Random

//class CommonUtil {
//
//}

/**
 * source
 */
case class SimulatedSource() extends RichSourceFunction[MarketUserBehavior] {
  var flag = true
  val behaviors = Seq("view", "click", "download", "install", "uninstall")
  val channels = Seq("appstore", "weibo", "wechat", "tieba")
  val rand = Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    val maxCounts = Long.MaxValue
    var count = 0L
    while (flag && count < maxCounts) {
      val id = UUID.randomUUID().toString
      val behavior = behaviors(rand.nextInt(behaviors.size))
      val channel = channels(rand.nextInt(channels.size))
      val ts = System.currentTimeMillis
      sourceContext.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      ThreadUtil.sleep(50)
    }
  }

  override def cancel(): Unit = flag = false
}