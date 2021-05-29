package com.scala.base

case class SubTask() extends Serializable {

  var data: List[Int] = _
  //  val logic = (n: Int) => n * 2
  var logic: (Int) => Int = _

  def compute() = {
    data.map(logic)
  }
}
