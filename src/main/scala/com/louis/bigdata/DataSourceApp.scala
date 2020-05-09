package com.louis.bigdata

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DataSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataSourceApp").getOrCreate()

//    text(spark)
//    json(spark)
//    common(spark)
//    parquet(spark)
//    convert(spark)
//    jdbc(spark)
    jdbc2(spark)
    spark.stop()
  }

  def jdbc2(spark: SparkSession): Unit ={
      import spark.implicits._
      val config = ConfigFactory.load()
      val driver = config.getString("db.default.driver")
      val url = config.getString("db.default.url")
      val user = config.getString("db.default.user")
      val pass = config.getString("db.default.pass")
      val sourcetable1 = config.getString("db.default.sourcetable1")
      val targettable1 = config.getString("db.default.targettable1")
      val sourcetable2 = config.getString("db.default.sourcetable2")
      val targettable2 = config.getString("db.default.targettable2")


      val jdbcDF1 = spark.read
        .format("jdbc")
        .option("driver",driver)
        .option("url", url)
        .option("dbtable", sourcetable1)
        .option("user", user)
        .option("password", pass)
        .load()
      jdbcDF1.show(false)

  }

  /**
   * the data is in mysql
   * read the data from mysql
   * load the data into mysql
   * @param spark
   */

  def jdbc(spark: SparkSession): Unit ={
    import spark.implicits._
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306?serverTimezone=PST"
    val user = "root"
    val pass = "12345678"
    val sourcetable1 = "data.facts"
    val targettable1 = "data.facts1"
    val sourcetable2 = "data.cities"
    val targettable2 = "data.cities1"
    val jdbcDF1 = spark.read
      .format("jdbc")
      .option("driver",driver)
      .option("url", url)
      .option("dbtable", sourcetable1)
      .option("user", user)
      .option("password", pass)
      .load()
//    jdbcDF1.show(false)

    jdbcDF1.filter("population > 3000000").write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", targettable1)
      .option("user", user)
      .option("password", pass)
      .save()

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", pass)
    val jdbcDF2 = spark.read
      .jdbc(url, sourcetable2, connectionProperties)
//    jdbcDF2.show(false)
    jdbcDF2.filter("capital = true").write
      .jdbc(url,targettable2,connectionProperties)
  }

  /**
   * input json
   * output parquet
   * @param spark
   */
  def convert(spark: SparkSession): Unit ={
    import spark.implicits._
    val jsonDF: DataFrame = spark.read.json("/Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.json")
    jsonDF.filter("age > 20").write.format("parquet").mode(SaveMode.Overwrite).save("out")
  }

  def parquet(spark: SparkSession): Unit ={
    import spark.implicits._
    val parquetDF: DataFrame = spark.read.parquet("/Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/users.parquet")
    parquetDF.printSchema()
    parquetDF.show()
    parquetDF.select("name","favorite_numbers")
      .write.mode(SaveMode.Overwrite)
      .option("compression","none")
      .parquet("out")
    spark.read.parquet("/Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/out").show()
  }

  def common(spark: SparkSession): Unit ={
    import spark.implicits._
    val textDF = spark.read.format("text").load("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.txt")
    val jsonDF = spark.read.format("json").load("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people2.json")

    jsonDF.write.format("json").mode("overwrite").save("out")
  }


  def json(spark: SparkSession): Unit ={
    import spark.implicits._
//    val jsonDF: DataFrame = spark.read.json("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.json")
//    jsonDF.show()
//
//    jsonDF.filter("age > 20").select("name").write.mode(SaveMode.Overwrite).json("out")
    val jsonDF2: DataFrame = spark.read.json("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people2.json")
    jsonDF2.select($"name",$"age",$"info.work".as("work"),$"info.home".as("home")).write.mode(SaveMode.Overwrite).json("out")


  }
  def text(spark: SparkSession): Unit ={
    import spark.implicits._
    val textDF: DataFrame = spark.read.text("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.txt")
    textDF.show()
    val result: Dataset[(String)] = textDF.map(x => {
      val splits: Array[String] = x.getString(0).split(",")
      (splits(0).trim)
    })
//    result.write.text("out")
    //    error will occur, text data source does not support int data type
    //    text data source support only a single column
    //    it will fail in the second time because there is already a output file
    //    use SaveMode to fix this error
//    result.write.mode(SaveMode.Overwrite).text("out")
    result.write.mode("overwrite").text("out")
//    result.write.mode(SaveMode.Append).text("out")
//    result.write.mode(SaveMode..ErrorIfExists).text("out")
//    result.write.save("out")
  }
}
