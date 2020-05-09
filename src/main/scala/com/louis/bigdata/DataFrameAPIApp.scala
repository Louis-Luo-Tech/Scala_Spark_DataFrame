package com.louis.bigdata

import org.apache.spark.sql.SparkSession

object DataFrameAPIApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataFrameAPIApp").getOrCreate()

    val people = spark.read.json("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.json")
    people.printSchema()
    people. show()
    spark.close()
  }
}
