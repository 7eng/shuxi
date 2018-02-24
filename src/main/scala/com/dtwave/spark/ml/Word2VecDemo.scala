package com.dtwave.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession


/**
  *
  * 机器学习算法: Word2Vector
  *
  * 输出Jar包路径: libs/spark_ml_demo.jar
  *
  * @author baisong
  * @date 18/2/1
  */
object Word2VecDemo {

  //设置日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    /**
      * 此处不用设置AppName和Master参数, 数栖平台提交作业会自动添加.
      *
      * AppName命名格式: 任务名_用户名_实例名
      */
    val spark = SparkSession
      .builder
      .getOrCreate()

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(6)
      .setMinCount(0)

    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)

    result.show(3, false)

    spark.stop()
  }
}
