package com.louis.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField,StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InteroperatingRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DatasetApp").getOrCreate()
//    runinferSchema(spark)
    runProgrammaticSchema(spark)
    spark.stop()
  }

  /**
   * the second method
   * @param spark
   */

  def runProgrammaticSchema (spark: SparkSession): Unit ={
    import spark.implicits._
    val peopleRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.txt")
    //Step1
    val peopleRowRDD: RDD[Row] = peopleRDD.map(_.split(","))
      .map(x => Row(x(0), x(1).trim.toInt))
    //Step2
    val struct =
         StructType(
             StructField("name", StringType, true) ::
               StructField("age", IntegerType, false) :: Nil)
    val peopleDF: DataFrame = spark.createDataFrame(peopleRowRDD, struct)
    peopleDF.show(false)

  }

  /**
   * the first method
   * 1) define case class
   * 2) RDD, map, convert each row in map to case class
   * @param spark
   */
  def runinferSchema(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.txt")

    val peopleDF: DataFrame = peopleRDD.map(_.split(","))
      .map(x => People(x(0), x(1).trim.toInt))
      .toDF()
    //    peopleDF.show(false)

    peopleDF.createOrReplaceTempView("people")

    val queryDF = spark.sql("select name, age from people where age between 19 and 29")
    //    queryDF.show(false)

    //    queryDF.map(x=>"name:"+x(0)).show(false) //from index

    queryDF.map(x => "name:" + x.getAs[String]("name")).show() //from field
  }

  case class People(name:String, age:Int)

}
