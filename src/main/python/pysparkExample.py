# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

"""
*********************************************************************
功能：使用pyspark格式化时间
时间：2018-02-01
*********************************************************************
"""

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('yarn') \
        .appName("") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.sql("select date_format('2016-06-20','yyyy/MM/dd') as dtwave_time")
df.show(10)

spark.stop()
