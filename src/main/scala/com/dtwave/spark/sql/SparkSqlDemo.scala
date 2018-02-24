package com.dtwave.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  *
  * SparkSQL定义临时函数、操作Hive表
  *
  * 输出Jar包路径: libs/spark_sql_demo.jar
  *
  * @author baisong
  * @date 18/2/1
  */
object SparkSqlDemo {

  //设置日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: SparkSqlDemo <dbName> <tableName>")
      System.exit(-1)
    }
    //库名
    val dbName = args(0)
    // 表名
    val tableName = args(1)

    /**
      * 此处不用设置AppName和Master参数, 数栖平台提交作业会自动添加.
      *
      * AppName命名格式: 人物名_用户名_实例名
      */
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    // 注册临时UDF
    // spark.udf.register("demo_udf", (x: String) => "Hello," + x)

    // 查询Hive表数据
    val df = spark.sql(s"select * from ${dbName}.${tableName} limit 3")

    // 打印输出数据
    df.show(false)
  }
}
