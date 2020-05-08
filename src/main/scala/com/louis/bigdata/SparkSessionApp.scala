package com.louis.bigdata

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
  }
}
