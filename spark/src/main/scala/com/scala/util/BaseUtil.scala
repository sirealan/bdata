package com.scala.util

import com.scala.bean.HotCategory
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BaseUtil {

  /**
   * 基础sc
   *
   * @param masterName
   * @param AppName
   * @return
   */
  def baseSc(masterName: String = "local[*]", AppName: String = "sc"): SparkContext = {
    new SparkContext(
      new SparkConf()
        .setMaster(masterName)
        .setAppName(AppName)
    )
  }

  /**
   * 商品热门分类topN 的分类ID
   *
   * @param rdd
   * @param topN
   */
  def topNCategory(rdd: RDD[String], topN: Int = 10) = {
    rdd
      .flatMap(o => {
        val split = o.split("_")
        if (split(6) != "-1") List((split(6), (1, 0, 0)))
        else if (split(8) != "null") split(8).split(",").map((_, (0, 1, 0)))
        else if (split(10) != "null") split(10).split(",").map((_, (0, 0, 1)))
        else Nil
      })
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .sortBy(_._2, false)
      .take(topN)
      .map(_._1)
  }
}

/**
 * 自定义累加器
 */
case class MyAccumulator() extends AccumulatorV2[String, collection.mutable.Map[String, Long]] {
  private var wcMap = collection.mutable.Map[String, Long]()

  override def isZero: Boolean = wcMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = MyAccumulator()

  override def reset(): Unit = wcMap.clear()

  override def add(v: String): Unit = wcMap.update(v, wcMap.getOrElse(v, 0L) + 1)

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1 = this.wcMap
    val map2 = other.value
    map2.foreach {
      case (w, c) => map1.update(w, map1.getOrElse(w, 0L) + c)
    }
  }
  //    other.value.foreach((x,y)=>wcMap.update(x,wcMap.getOrElse(x,0L)+y))


  override def value: mutable.Map[String, Long] = wcMap
}

/**
 * 自定义 HotCategory累加器
 * IN: （品类ID，行为类型）
 * OUT: mutable.Map[String, HotCategory]
 */
case class HotCategoryAccumulator() extends AccumulatorV2[(String, String), collection.mutable.Map[String, HotCategory]] {
  private var map = collection.mutable.Map[String, HotCategory]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = HotCategoryAccumulator()

  override def reset(): Unit = map.clear()

  override def add(v: (String, String)): Unit = {
    val cid = v._1
    val acitonType = v._2
    val category = map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
    if (acitonType == "click") category.clickCnt += 1
    else if (acitonType == "order") category.orderCnt += 1
    else if (acitonType == "pay") category.payCnt += 1
    map.update(cid, category)
  }


  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    other
      .value
      .foreach {
        case (cid, aa) => {
          val bb = map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          bb.clickCnt += aa.clickCnt
          bb.orderCnt += aa.orderCnt
          bb.payCnt += aa.payCnt
          map.update(cid, bb)
        }
      }
  }

  override def value: mutable.Map[String, HotCategory] = map
}

///**
// * 自定义 HotCategory下的SessionTopN的累加器
// * IN: （品类ID，行为类型）
// * OUT: mutable.Map[String, HotCategory]
// */
//case class HotCategoryTopNSessionTopNAccumulator(actionRdd:RDD[String]) extends AccumulatorV2[(String, String), collection.mutable.Map[String, HotCategory]] {
//  private var map = collection.mutable.Map[String, HotCategory]()
//
//  override def isZero: Boolean = map.isEmpty
//
//  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = HotCategoryAccumulator()
//
//  override def reset(): Unit = map.clear()
//
//  override def add(v: (String, String)): Unit = {
//    val cid = v._1
//    val acitonType = v._2
//    val category = map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
//    if (acitonType == "click") category.clickCnt += 1
//    else if (acitonType == "order") category.orderCnt += 1
//    else if (acitonType == "pay") category.payCnt += 1
//    map.update(cid, category)
//  }
//
//
//  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
//    other
//      .value
//      .foreach {
//        case (cid, aa) => {
//          val bb = map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
//          bb.clickCnt += aa.clickCnt
//          bb.orderCnt += aa.orderCnt
//          bb.payCnt += aa.payCnt
//          map.update(cid, bb)
//        }
//      }
//  }
//
//  override def value: mutable.Map[String, HotCategory] = map
//}
