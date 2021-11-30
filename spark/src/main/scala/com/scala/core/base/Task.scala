package com.scala.core.base

case class Task() extends Serializable {

  val data = List(1, 2, 3, 4)
  //  val logic = (n: Int) => n * 2
  val logic: (Int) => Int = _ * 2

  def compute() = {
    data.map(logic)
  }
}
