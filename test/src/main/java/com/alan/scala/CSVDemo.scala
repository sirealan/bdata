package com.alan.scala

import avro.shaded.com.google.common.collect.ImmutableBiMap

import java.util

object CSVDemo extends App {
  import sys.process._
  import scala.language.postfixOps
  "ls -al" !

  "echo hello > aa1.sh" !

  val aa = "curl baidu.com" !!

  s"$aa > a.md" !

  "ls -al" #| "grep Foo" !

  Seq.fill(10)(Seq.fill(3)("Foo"))
  case class Foo(i: Int, s0: String, s1: Seq[String])
  Foo(1, "", Nil)
  Foo(
     1234567,
     "I am a cow, hear me moo",
     Seq("I weigh twice as much as you", "and I look good on the barbecue")
   )
  val a ={
    val x=23
    val y=23
    x+y
  }
//  import $ivy.`org.scalaz::scalaz-core:7.2.27`, scalaz._, Scalaz._
//  import $ivy.`com.google.guava:guava:18.0`, com.google.common.collect._
val bimap = ImmutableBiMap.of(1, "one", 2, "two", 3, "three")
//  ReplAPI
  (Some(1).map _)
//  source(new java.util.ArrayList())
//  Seq(1, 2, 3).map(x => x.)
//  interp.configureCompiler(_.settings.language.tryToSet(List("dynamics")))
//  import ammonite.ops._
//  import ammonite.ops.ImplicitWd._
//  %
  priwntln("Month, Income, Expenses, Profit")
}
