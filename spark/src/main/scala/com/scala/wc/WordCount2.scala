package com.scala.wc

import org.apache.spark.{SparkConf, SparkContext}

//sc.textFile("/user/root/wc.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect

object WordCount2 extends App {
//  new StaticLoggerBinder()
  val sc = new SparkContext(
    new SparkConf()
      .setMaster("local")
      .setAppName("wc")

  )
  sc
    .textFile("datas")
    .flatMap(_.split(" "))
    .map((_, 1))
    // goupy + map 分组加聚合的统一方法reduceByKey
    .reduceByKey(_+_)
//    .groupBy(_._1)
//    .map(_._2.reduce((a, b) => (a._1, a._2 + b._2)))
    //    .map {
    //      case (word, list) =>
    //        list.reduce((a, b) => (a._1, a._2 + b._2))
    //    }
    .collect
    .foreach(println)
  sc.stop
//  sc.textFile("datas").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

  }
