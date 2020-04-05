package com.company.Framework.scalaclasses
import org.apache.spark.sql.{ColumnName,DataFrame,SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
class Test {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkExample")
    .config("spark.driver.allowMultipleContexts","true")
    .getOrCreate()
  import spark.implicits._

  def getData(): DataFrame ={
    val df = spark.read.format("csv").option("header", "true").load("/Users/xiangluo/Desktop/data/test.csv")
    df
  }

}
