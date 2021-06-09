package com.flink.examples


import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/**
 * kafka生产者:
 * kafka-console-producer.sh --broker-list hd1:9092,hd2:9092,hd3:9092 --topic sensor
 */
object TableApiTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //  val ds = env.readTextFile("input/sensor.txt")
  //    .map(o => {
  //      val split = o.split(",")
  //      SensorReader(
  //        split(0),
  //        split(1),
  //        split(2).toDouble
  //      )
  //    })
  val settings = EnvironmentSettings
    .newInstance
    .useOldPlanner
    .inStreamingMode
    .build
  val tenv = StreamTableEnvironment.create(env, settings)
  //  val oldStreamEnv = StreamTableEnvironment.create(env, settings)
  //  val benv = ExecutionEnvironment.getExecutionEnvironment
  //  val oldBatchEnv = BatchTableEnvironment.create(benv)
  //
  //  val blinkSettings = EnvironmentSettings
  //    .newInstance
  //    .useBlinkPlanner
  //    .inStreamingMode
  //    .build
  //  val blinkStreamEnv = StreamTableEnvironment.create(env, blinkSettings)
  //
  //  val blinkBatchSettings = EnvironmentSettings
  //    .newInstance
  //    .useBlinkPlanner
  //    .inBatchMode
  //    .build
  //  val blinkBatchEnv = TableEnvironment.create(blinkBatchSettings)
  //  tenv.connect(
  //    new FileSystem().path("input/sensor.txt")
  //  )
  //    .withFormat(new Csv)
  //    .withSchema(new Schema()
  //      .field("id", DataTypes.STRING())
  //      .field("ts", DataTypes.STRING())
  //      .field("temperature", DataTypes.DOUBLE())
  //    )
  //    .createTemporaryTable("sensor")
  //
  //  tenv
  //    .from("sensor")
  //    .toAppendStream[SensorReader]
  //    .print("sensor")
  // 从本地文件读取数据
  tenv.connect(new FileSystem().path("input/sensor.txt"))
    .withFormat(new Csv())
    .withSchema(
      new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("sensor")
  // 从kafka读取数据
  tenv.connect(
    new Kafka()
      //      .version("2.8.0")
      .version("universal")
      .topic("sensor")
      .property("zookeeper.connect", "hd1:2181,hd2:2181,hd3:2181")
      .property("bootstrap.servers", "hd1:9092,hd2:9092,hd3:9092")
  )
//        .withFormat(new Json())
    .withFormat(new Csv())
    .withSchema(
      new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("ksensor")
  // table api
  //  tenv
  //    .from("ksensor")
  ////    .filter('id,temperature)
  //    .filter('id=="ws_002")
  // sql api
  tenv.sqlQuery(
    """
      |select id,temperature
      |from ksensor
      |where id = 'ws_002'
      """.stripMargin
  )
    .toAppendStream[(String,Double)]
    .print("sql")
  tenv
    //    .from("sensor")
    .from("ksensor")
    .toAppendStream[(String, String, Double)]
    .print("sensor")
  env.execute
}