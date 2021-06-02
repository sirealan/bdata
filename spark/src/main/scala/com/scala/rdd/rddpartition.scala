package com.scala.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Fractional.Implicits.infixFractionalOps
import scala.math.Integral.Implicits.infixIntegralOps

/**
 */
object rddpartition extends App {
  val sc = new SparkContext(
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
  )
  //  val a =
  sc
    //        .textFile("input/a.txt", 2)
    // 3分区的最大值，结果是2、4、6
    //    .makeRDD(Seq(1,2,3,4,5,6),3)
    //    .mapPartitions(o=>List(o.max).iterator)
    // 获取n个分区中指定分区的最大值，结果是6
    //    .makeRDD(1 to 6, 3)
    //    .mapPartitionsWithIndex((i, it) =>
    //      if (i == 2) List(it.max).iterator
    //      else Nil.iterator
    //    )
    // 获取n个分区中分区最大值且求和，如下面就是2+4+6=12
    //      .makeRDD(1 to 6, 3)
    //      .mapPartitions(o => List(o.max).iterator)
    //    .glom()
    //      .collect
    //      .sum
    //  println(a)
    // 抽取样本
    //    .makeRDD(1 to 10)
    //    .sample(false,0.4,1)
    // 去重distinct 底层:reduceByKey+map
    // _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    //    .makeRDD(1 to 10)
    //    .distinct
    // coalesce
    //    .makeRDD(1 to 6, 3)
    //    .coalesce(2)// 1-2，3-4-5-6
    //    .coalesce(2, true)// 打乱分区实现均衡，两边都是3个数字
    /*
    查出分区内最大值且聚合
    柯里化函数（参数1：分区内规则，参数2：分区外规则）
    // 0 就是2+4等于6
    // 3 就是3+4等于7
    // 5 就是5+5等于10
     */
    .makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    //    .aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => x + y)
    //    .aggregateByKey(0)(_+_,_+_) //分区相加
    //    .foldByKey(0)(_+_)// 如果分区内和分区间规则一样，可以使用这个简化
    //获取每隔key的平均值
    .aggregateByKey((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    .mapValues({ case (x, y) => x / y })
    .collect
    .foreach(println)
  //    .saveAsTextFile("output") // 保存分区文件，分几份保存几份


  sc.stop()
}
