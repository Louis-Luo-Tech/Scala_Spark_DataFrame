package com.company.Framework.scalaclasses

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

class RDDFrame {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkExample")
      .config("spark.driver.allowMultipleContexts","true")
      .getOrCreate()
    import spark.implicits._
    def getRDD(): RDD[String] ={
    val data = Array(1,2,3,4,5,6)
    val distdata = spark.sparkContext.parallelize(data)

    val distFile = spark.sparkContext.textFile("/Users/xiangluo/Desktop/data/data2.csv")
      distFile
  }
}
