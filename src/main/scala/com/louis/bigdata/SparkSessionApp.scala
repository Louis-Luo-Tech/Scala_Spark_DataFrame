package com.louis.bigdata

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSessionApp").getOrCreate()
    val df = spark.read.text("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/input.txt")
    df.printSchema()
    df.show()
    spark.stop()
  }
}
