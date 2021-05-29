package com.scala.base

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

// Driver类似客户端
object Driver extends App {
  //  val client = new Socket("localhost", 9999)
  //  private val stream: OutputStream = client.getOutputStream
  //  private val objOut = new ObjectOutputStream(stream)
  //  objOut.writeObject(Task())
  //  objOut.flush()
  //  objOut.close()
  //  client.close()

  //两个subtask版
  val client1 = new Socket("localhost", 9999)
  val client2 = new Socket("localhost", 8888)
  val task = Task()
  private val stream1: OutputStream = client1.getOutputStream
  private val objOut1 = new ObjectOutputStream(stream1)
  val subTask1 = SubTask()
  subTask1.logic = task.logic
  subTask1.data = task.data.take(2)
  objOut1.writeObject(subTask1)
  objOut1.flush()
  objOut1.close()
  client1.close()

  private val stream2: OutputStream = client2.getOutputStream
  private val objOut2 = new ObjectOutputStream(stream2)
  val subTask2 = SubTask()
  subTask2.logic = task.logic
  subTask2.data = task.data.takeRight(2)
  objOut2.writeObject(subTask2)
  objOut2.flush()
  objOut2.close()
  client2.close()
  println("send message is successfully")
}
