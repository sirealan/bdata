package com.scala.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 1.并行度可以通过set("spark.default.parallelism", "5")设置，也可以通过.makeRDD(Seq, 2)
 * set优先度更高，如果数据只有4份，你设置并行度为5，照样只生成4份，分区numSlices
 * makeRDD(Seq(1, 2, 3, 4),3) 1,2,3/4
 * makeRDD(Seq(1, 2, 3, 4,5),3) 1,2/3,4/5
 * ParallelCollectionPartition.positions
 * 0-(0,1)[1] 1-(1,3)[2,3] 2-(3,5)[4,5] PS:左开右闭
 * (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
 */
object BaseRDD extends App {
//  new ParallelCollectionRDD()
  val sc = new SparkContext(
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
      .set("spark.default.parallelism", "5") // 设置并行度
  )
  private val value: RDD[Int] = sc
    .makeRDD(Seq(1, 2, 3, 4), 3)
  sc
    //    .parallelize(Seq(1,2,3,4))
    // numSlices切片
    //    .makeRDD(Seq(1, 2, 3, 4), 2)
    .makeRDD(Seq(1, 2, 3, 4),3)
    // textFile以行为单位， wholeTextFiles以文件为单位
    //    .textFile("hdfs://hd1:8020/user/root/wc.txt")
    //    .wholeTextFiles("hdfs://hd1:8020/user/root/wc.txt")
    //    .map(_ * 2)
    //    .collect
    //    .foreach(println)
    .saveAsTextFile("output") // 保存分区文件，分几份保存几份
  sc.stop()
}
