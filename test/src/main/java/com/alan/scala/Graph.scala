package com.alan.scala


import java.io.File
import java.util.Scanner

import scala.collection.mutable.TreeSet

class Graph {
  var V = 0 // 顶点
  var E = 0 // 边
  var adj: Array[TreeSet[Int]] = _ // 红黑树数组

  def this(filename: String) {
    this()
    val scanner = new Scanner(new File(filename))
    V = scanner.nextInt()
    adj = Array.ofDim[TreeSet[Int]](V)
    for (i <- 0 until V) {
      adj(i) = TreeSet.empty
    }
    E = scanner.nextInt()
    for (i <- 0 until E) {
      val a = scanner.nextInt()
      val b = scanner.nextInt()
      //println(s"$a + $b")
      var as = adj(a)
      //if (as == null) as = TreeSet.empty
      as.add(b)
      adj(a) = as
      var bs = adj(b)
      //if (bs == null) bs = TreeSet.empty
      bs.add(a)
      adj(b) = bs
    }
  }

  override def toString: String = {
    val sb = new StringBuffer()
    for (i <- 0 until adj.length) {
      sb.append(s"顶点 $i ->\t")
      if (adj(i) != null) {
        for (y <- adj(i)) {
          sb.append(s"$y\t")
        }
      }
      sb.append("\n")
    }
    sb.toString
  }

  /**
   * 获得 图中顶点的个数
   *
   * @return
   */
  def getV() = {
    V
  }

  /**
   * 获得 图中边的个数
   *
   * @return
   */
  def getE() = {
    E
  }


  /**
   * 两个顶点间是否有边
   *
   * @param v
   * @param w
   * @return
   */
  def hasEdge(v: Int, w: Int) = {
    adj(v).contains(w)
  }

  /**
   * 获取v的所有邻边
   *
   * @param v
   * @return
   */
  def getAdjacentSide(v: Int) = {
    if (adj(v) != null) {
      adj(v).toList
    } else {
      List.empty
    }
  }
}

object Graph extends App {
    val graph = Graph("./data/graph/g.txt")
    println(graph)

    println("-------------------------")
    println(s"顶点个数：${graph.getV()}")
    println("-------------------------")
    println(s"边的个数：${graph.getE()}")
    println("-------------------------")
    println(s"2的邻边：${graph.getAdjacentSide(2)}")
    println("-------------------------")
    println(s"1 和 2 是否有边：${graph.hasEdge(1, 2)}")

  def apply(filename: String): Graph = new Graph(filename)
}


