package com.dtwave.flink

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource
import org.apache.flink.table.api.{Table, TableEnvironment, Types}

/**
  * FlinkDemo 演示程序。
  * 消费kafka orders topic的内容。
  * 输出到控制台。
  */
object FlinkDemo {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: FlinkDemo <bootstrap.servers>\n" +
        "Example : mq1:9092,mq2:9092,mq3:9092")
      System.exit(-1)
    }

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv=TableEnvironment.getTableEnvironment(env)

    //设置检查点
    env.enableCheckpointing(5000L)
    //与kafka集成
    val props = new Properties()
    props.setProperty("bootstrap.servers", args.apply(0))
    props.setProperty("group.id", "flink-group")

    val typeInfo=Types.ROW(
      Array[String]("_id","orderId","proName","amount","orderTime"),
      Array[TypeInformation[_]] (Types.LONG,Types.LONG,Types.STRING,Types.INT,Types.LONG)
    )
    //kafka source 选择消费orders这个topic
    val kafkaSource=new Kafka010JsonTableSource("orders",props,typeInfo)
    //注册为表
    tableEnv.registerTableSource("orders",kafkaSource)
    //appended table
    val appendedTable:Table=tableEnv.sql("select orderId, proName, amount from orders")
    val appendedStream=tableEnv.toAppendStream[(Long,String,Int)](appendedTable)

    //输出到标准输出
    appendedStream.print()
    //开始执行
    env.execute("orders in sql")
  }
  case class Order(orderId:Int,proName:String,amount:Int)
}
