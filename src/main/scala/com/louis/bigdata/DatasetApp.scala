package com.louis.bigdata

import org.apache.spark.sql.{Dataset, SparkSession}

object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DatasetApp").getOrCreate()
    import spark.implicits._

    val ds = Seq(Person("louis", "30")).toDS()
    ds.show()

    val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
    primitiveDS.map(x => x+1).collect().foreach(println)

    val peopleDF = spark.read.json("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.json")
    val peopleDS: Dataset[Person] = peopleDF.as[Person]
//    peopleDS.show(10,false)


//    peopleDF.select("ame").show()   //The error will occur when it is running
//    peopleDS.map(x => x.nam).show()  // The error will occur when it is compiling

    spark.stop()
  }

  case class Person(name:String, age:String)
}
