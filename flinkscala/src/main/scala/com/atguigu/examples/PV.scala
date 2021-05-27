package com.atguigu.examples


import org.apache.flink.streaming.api.scala._

object PV extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.readTextFile("input/UserBehavior.csv")
    //    .map(o=>{
    //      val split = o.split(",")
    //      UserBehavior(
    //        split(0).toLong,
    //        split(1).toLong,
    //        split(2).toInt,
    //        split(3),
    //        split(4).toLong
    //      )
    //    })
    //    .filter(_.behavior.equals("pv"))
    //    .map(o=>(o.behavior,1))
    .flatMap(_.split(" "))
    .map(o => {
      val split = o.split(",")
      split(3) match {
        case "pv" => (split(0),1)
        case _ => ("", 0)
      }
    })
    .keyBy(_._1)
//    .process(new UserBehaviorFunction())
    .sum(1)
    .print()
  env.execute()
  //  .readTextFile("input/UserBehavior.csv")
  //    .map(line -> { // 对数据切割, 然后封装到POJO中
  //      String[] split = line.split(",");
  //      return new UserBehavior(
  //        Long.valueOf(split[0]),
  //        Long.valueOf(split[1]),
  //        Integer.valueOf(split[2]),
  //        split[3],
  //        Long.valueOf(split[4]));
  //    })
  //    .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
  //    .map(behavior -> Tuple2.of("pv", 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG)) // 使用Tuple类型, 方便后面求和
  //    .keyBy(value -> value.f0)  // keyBy: 按照key分组
  //    .sum(1) // 求和
  //    .print();


}
