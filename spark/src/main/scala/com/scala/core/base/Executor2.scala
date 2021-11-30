package com.scala.core.base

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

// Executor类似server，两个Executor对于两个subtask
object Executor2 extends App {

  val server = new ServerSocket(8888)
  println("服务器启动~接受")
  private val socket: Socket = server.accept() // 等待连接
  private val stream: InputStream = socket.getInputStream
  private val objIn = new ObjectInputStream(stream)
  private val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
  println(s"节点${server.getLocalPort}接收到结算结果数据: ${task.compute}")
  objIn.close()
  socket.close()
  server.close()
  //    .wr
}
