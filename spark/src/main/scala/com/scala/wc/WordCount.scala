package com.scala.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val sc = new SparkContext(
    new SparkConf()
      .setMaster("local")
      .setAppName("WordCount"))
  sc
    .textFile("datas")
    .flatMap(_.split(" "))
    .groupBy(o => o)
    .map(o => (o._1, o._2.size))
    //    .map { case (word, list) => (word, list.size) }
    .collect
    .foreach(println)
  sc.stop()
}
