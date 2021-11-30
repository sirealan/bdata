package com.scala.sql

import com.scala.util.BaseUtil
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object BaseExample {

  def sqlBase {
    val spark = BaseUtil.baseSqlSc()
    import spark.implicits._
    val df = spark.read.json("input/user.json")
    df.createOrReplaceTempView("user")
    //  df.show()
    //  df.schema.printTreeString()
    spark.sql("select * from user").show
    spark.sql("select name from user").show
    spark.sql("select avg(age) from user").show
    df.select('age + 1).show()

    spark.close()
  }

  def udf {
    val spark = BaseUtil.baseSqlSc()
    import spark.implicits._
    val df = spark.read.json("input/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("prefixName", (name: String) => "Name: " + name)
    spark.sql("select age, prefixName(name) from user").show
    spark.close()
  }

  def udaf {
    val spark = BaseUtil.baseSqlSc()
    import spark.implicits._
    val df = spark.read.json("input/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg", MyAvgUDAF())

    spark.sql("select ageAvg(age) from user").show

    spark.close()
  }

  def jdbc {
    val spark = BaseUtil.baseSqlSc()
    import spark.implicits._
    // 读取MySQL数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .load()
    //df.show

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()
    spark.close()
  }

  def hive {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = BaseUtil.baseSqlSc()
    import spark.implicits._
    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-size.xml文件到classpath下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系（包含MySQL驱动）
    spark.sql("show tables").show
    spark.close()
  }




}

/*
     自定义聚合函数类：计算年龄的平均值
     1. 继承UserDefinedAggregateFunction
     2. 重写方法(8)
     */
case class MyAvgUDAF() extends UserDefinedAggregateFunction {
  // 输入数据的结构 : Int
  override def inputSchema: StructType = {
    StructType(
      Array(
        StructField("age", LongType)
      )
    )
  }

  // 缓冲区数据的结构 : Buffer
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("total", LongType),
        StructField("count", LongType)
      )
    )
  }

  // 函数计算结果的数据类型：Out
  override def dataType: DataType = LongType

  // 函数的稳定性
  override def deterministic: Boolean = true

  // 缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //buffer(0) = 0L
    //buffer(1) = 0L

    buffer.update(0, 0L)
    buffer.update(1, 0L)
  }

  // 根据输入的值更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getLong(0) + input.getLong(0))
    buffer.update(1, buffer.getLong(1) + 1)
  }

  // 缓冲区数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
    buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
  }

  // 计算平均值
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0) / buffer.getLong(1)
  }
}