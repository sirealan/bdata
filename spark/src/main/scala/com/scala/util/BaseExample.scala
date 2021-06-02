package com.scala.util

import com.scala.bean.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.AccumulatorV2

object BaseExample {

  def adtopN {
    val sc = BaseUtil.baseSc()
    sc
      .textFile("input/agent.log")
      .map(o => {
        val split = o.split(" ")
        ((split(0), (split(4))), 1) // (省份，广告),1
      })
      .reduceByKey(_ + _) // (省份，广告),sum
      .map(o => (o._1._1, (o._1._2, o._2))) //省份，(广告,sum)
      .groupByKey() // .groupBy(_._1) 省份，[(广告1,sum),(广告2,sum)]
      // 倒序前三
      .mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
      .collect
      .foreach(println)
    sc.stop()
  }

  /**
   * 累加器
   */
  def accBase {
    val sc = BaseUtil.baseSc()
    val sumACC = sc.longAccumulator("sumACC")
    sc
      .parallelize(List(1, 2, 3, 4))
      .foreach(sumACC.add(_))
    println(sumACC.value)
    sc.stop
  }

  /**
   * 累加器WC
   */
  def accWc {
    val sc = BaseUtil.baseSc()
    val myacc = MyAccumulator()
    sc.register(myacc, "wordcountAcc")
    sc
      .parallelize(List("spark", "hello", "hello"))
      .foreach(myacc.add(_))
    println(myacc.value)
    sc.stop

  }

  /**
   * 广播变量
   */
  def broadcast {
    val sc = BaseUtil.baseSc()
    val map = collection.mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //    val bc: Broadcast[collection.mutable.Map[String, Int]] = sc.broadcast(map)
    val bc = sc.broadcast(map)
    sc
      .parallelize(List(
        ("a", 1), ("b", 2), ("c", 3)
      ))
      // 耗费性能，换一种
      //    .join(
      //      sc
      //      .parallelize(List(
      //        ("a", 4), ("b", 5), ("c", 6)
      //      ))
      //    )
      // 非广播变量
      //      .map{case (w,c)=>(w,(c,map.getOrElse(w,0)))}
      .map { case (w, c) => (w, (c, bc.value.getOrElse(w, 0))) }
      .collect
      .foreach(println)
    sc.stop

  }

  /**
   * 热门商品分类 点击数+下单数+支付数
   */
  def hotCategoryTopN {
    val sc = BaseUtil.baseSc()
    val data = sc
      .textFile("input/user_visit_action.txt")
      .map(o => {
        val split = o.split("_")
        (
          split(0), split(1), split(2), split(3), split(4), split(5), split(6),
          split(7), split(8), split(9), split(10)
        )
      })
    // 点击
    val click = data
      .filter(_._7 != "-1")
      .map(o => (o._7, 1))
      .reduceByKey(_ + _)
    // 下单
    val order = data
      .filter(_._9 != "null")
      // 一个订单有多个商品，且用,分割
      .flatMap(_._9.split(",").map(id => (id, 1)))
      .reduceByKey(_ + _)
    // 支付
    val pay = data
      .filter(_._11 != "null")
      // 一个订单有多个商品，且用,分割
      .flatMap(_._11.split(",").map(id => (id, 1)))
      .reduceByKey(_ + _)
    click
      .cogroup(order, pay)
      .mapValues(o => (o._1.sum, o._2.sum, o._3.sum))
      //      .mapValues {
      //        case (a, b, c) => {
      ////          var clickcount = 0
      ////          a.foreach(clickcount += _)
      ////          var orderCount = 0
      ////          b.foreach(orderCount += _)
      ////          var payCount = 0
      ////          c.foreach(payCount += _)
      ////          (clickcount, orderCount, payCount)
      //          (a.sum, b.sum, c.sum)
      //        }
      //      }
      .sortBy(_._2, false).take(10)
      .foreach(println)
    sc.stop
  }

  /**
   * 热门商品分类 点击数+下单数+支付数 优化版本
   * 1.初始数据data重复读取，加persist、cache、saveAs**File解决
   */
  def hotCategoryTopN2 {
    val sc = BaseUtil.baseSc()
    val data = sc
      .textFile("input/user_visit_action.txt")
      .map(o => {
        val split = o.split("_")
        (
          split(0), split(1), split(2), split(3), split(4), split(5), split(6),
          split(7), split(8), split(9), split(10)
        )
      })
    data.cache
    // 点击
    val click = data
      .filter(_._7 != "-1")
      .map(o => (o._7, 1))
      .reduceByKey(_ + _)
    // 下单
    val order = data
      .filter(_._9 != "null")
      // 一个订单有多个商品，且用,分割
      .flatMap(_._9.split(",").map(id => (id, 1)))
      .reduceByKey(_ + _)
    // 支付
    val pay = data
      .filter(_._11 != "null")
      // 一个订单有多个商品，且用,分割
      .flatMap(_._11.split(",").map(id => (id, 1)))
      .reduceByKey(_ + _)
    click.map(o => (o._1, (o._2, 0, 0)))
      .union(order.map(o => (o._1, (0, o._2, 0))))
      .union(pay.map(o => (o._1, (0, 0, o._2))))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .sortBy(_._2, false).take(10)
      .foreach(println)
    sc.stop
  }

  /**
   * 热门商品分类 点击数+下单数+支付数 优化版本2
   * 存在大量的shuffle操作reduceByKey操作
   */
  def hotCategoryTopN3 {
    val sc = BaseUtil.baseSc()
    val data = sc
      .textFile("input/user_visit_action.txt")
      .flatMap(o => {
        val split = o.split("_")
        if (split(6) != "-1") List((split(6), (1, 0, 0)))
        else if (split(8) != "null") split(8).split(",").map((_, (0, 1, 0)))
        else if (split(10) != "null") split(10).split(",").map((_, (0, 0, 1)))
        else Nil
      })
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)
    sc.stop
  }

  /**
   * 热门商品分类 点击数+下单数+支付数 优化版本3
   * 累加器实现
   */
  def hotCategoryTopN4 {
    val sc = BaseUtil.baseSc()
    val acc = HotCategoryAccumulator()
    sc.register(acc, "hot-category")
    val data = sc
      .textFile("input/user_visit_action.txt")
      .foreach(o => {
        val split = o.split("_")
        if (split(6) != "-1") acc.add((split(6), "click"))
        else if (split(8) != "null") split(8).split(",").foreach(acc.add(_, "order"))
        else if (split(10) != "null") split(10).split(",").foreach(acc.add(_, "pay"))
      })
    acc
      .value
      .map(_._2)
      .toList
      .sortWith(
        (a, b) =>
          if (a.clickCnt > b.clickCnt) true
          else if (a.clickCnt == b.clickCnt)
            if (a.orderCnt > b.orderCnt) true
            else if (a.orderCnt == b.orderCnt)
              a.payCnt > b.payCnt
            //              if (a.payCnt > b.payCnt) true
            //              else false
            else false
          else false
      )
      .take(10)
      .foreach(println)
    sc.stop
  }

  /**
   * 热门商品分类TopN 点击数+下单数+支付数
   * 的情况下再加上session TopN
   */
  def hotCategoryTopNSessionTopN {
    val sc = BaseUtil.baseSc()
    val data = sc
      .textFile("input/user_visit_action.txt")
    data.cache
    val ids = BaseUtil.topNCategory(data)
    data
      //          .filter(o => {
      //            val split = o.split("_")
      //            if (split(6) != "-1") ids.contains(split(6))
      //            else false
      //          })
      //          .map(o => {
      //            val split = o.split("_")
      //            ((split(6), split(2)), 1)
      //          })
      // 上面的filter+map 可以使用下面的map+collect更简介
      .map(_.split("_"))
      .collect {
        case split if split(6) != "-1"
          && ids.contains(split(6)) => ((split(6), split(2)), 1)
      }
      .reduceByKey(_ + _)
      .map(o => (o._1._1, (o._1._2, o._2)))
      .groupByKey
      .mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))
      .collect()
      .foreach(println)
    sc.stop
  }

  /**
   * 页面跳转率计算 PageflowAnalysis
   */
  def pageflowAnalysis {
    val sc = BaseUtil.baseSc()
    val actionDataRDD = sc
      .textFile("input/user_visit_action.txt")
      .map(o => {
        val split = o.split("_")
        UserVisitAction(
          split(0),
          split(1).toLong,
          split(2),
          split(3).toLong,
          split(4),
          split(5),
          split(6).toLong,
          split(7).toLong,
          split(8),
          split(9),
          split(10),
          split(11),
          split(12).toLong
        )
      })
    actionDataRDD.cache
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    // List((1,2), (2,3), (3,4), (4,5), (5,6), (6,7))
    val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)
    // 分母
    val aa = actionDataRDD
      .filter(o => ids.init.contains(o.page_id))
      .map(o => (o.page_id, 1L))
      .reduceByKey(_ + _)
      .collect()
      .toMap
    // 分子
    val bb = actionDataRDD
      .groupBy(_.session_id)
      .mapValues(o => {
        val sortList = o.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageflowIds
          .filter(okflowIds.contains(_))
          .map((_, 1))
      })
      .map(_._2)
      .flatMap(w=>w)
      .reduceByKey(_+_)

    // 分子除以分母
    bb.foreach{
      case ( (pageid1, pageid2), sum ) => {
        val lon: Long = aa.getOrElse(pageid1, 0L)
        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + ( sum.toDouble/lon ))
      }
    }
    sc.stop
  }
}
