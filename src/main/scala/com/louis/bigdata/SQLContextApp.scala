package com.louis.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("sqlcontext").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.text("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/input.txt")
    df.show()
    sc.stop()
  }

}
