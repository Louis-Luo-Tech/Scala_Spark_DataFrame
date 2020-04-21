package com.company.Framework.scalaclasses

import org.apache.spark.sql.{ColumnName,DataFrame,SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


class Frame {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkExample")
    .config("spark.driver.allowMultipleContexts","true")
    .getOrCreate()
  import spark.implicits._
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/data?serverTimezone=PST"
  val user = "root"
  val pass = "12345678"
  val sourcetable1 = "facts"
  val sourcetable2 = "cities"
  def getData(): Option[DataFrame] ={
    // JDBC Connection and load table in Dataframe
    val facts = spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", sourcetable1)
      .option("user", user)
      .option("password", pass)
      .load()
    facts.show()

    val cities0 = spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", sourcetable2)
      .option("user", user)
      .option("password", pass)
      .load()
    val old_columns = cities0.columns
    val new_columns = old_columns.map(f => f+"_cities")
    val cities = cities0.toDF(new_columns:_*)
    cities.show()

//    val df3 = spark.read.table()

    val joined_tale = facts.as("t1").join(cities.as("t2"), ($"t1.id" === $"t2.facts_id_cities"),"left")
      .drop("facts_id")
    joined_tale.show()

    joined_tale.createOrReplaceTempView("myTempView")
    val sqlDF = spark.sql("SELECT sum(population) FROM myTempView group by id")
    sqlDF
  }
}
