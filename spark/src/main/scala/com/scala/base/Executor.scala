package com.scala.base

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}
// Executor类似server
object Executor extends App {

  val server = new ServerSocket(9999)
  println("服务器启动~接受")
  private val socket: Socket = server.accept() // 等待连接
  private val stream: InputStream = socket.getInputStream
  private val objIn = new ObjectInputStream(stream)
//  private val task: Task = objIn.readObject().asInstanceOf[Task]
  private val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
  println(s"节点${server.getLocalPort}接收到结算结果数据: ${task.compute}")
  objIn.close()
  socket.close()
  server.close()
//    .wr
}
