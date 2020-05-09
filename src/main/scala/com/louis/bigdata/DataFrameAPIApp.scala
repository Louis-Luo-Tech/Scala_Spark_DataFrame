package com.louis.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameAPIApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataFrameAPIApp").getOrCreate()

    import spark.implicits._

//    val people = spark.read.json("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/people.json")
//    people.printSchema()
//    people. show()
//
//    people.select("name").show()
//    people.select($"name").show()
//    people.filter($"age" > 21).show()
//    people.filter("age > 21").show()
//    people.groupBy("age").count().show()
//    +----+-----+
//    | age|count|
//    +----+-----+
//    |  19|    1|
//    |null|    1|
//    |  30|    1|
//    +----+-----+

//    people.createOrReplaceTempView("people")
//    spark.sql("select name from people where age > 21").show()
//
//    people.select($"name",($"age"+10).as("new_age")).show()

    val zips = spark.read.json("file:///Users/xiangluo/Documents/GitHub/Scala_Spark_DataFrame/input/zips.json")
//    zips.printSchema()
//    zips.show(false) //default top 20 rows  The information is not shown completely
//    zips.head(3).foreach(println)
//
//    zips.first()
//    zips.take(5)
//
//    val count = zips.count()
//    println(s"Total Count: $count") //wc -l zips.json
      zips.filter("pop > 4000").show(10)
      zips.filter($"pop" > 4000).show()
//    zips.filter(zips.col("pop") > 40000).show(10)
//    zips.filter(col("pop") > 40000).show(10)  //import org.apache.spark.sql.functions._
//    zips.filter(zips.col("pop") > 40000).withColumnRenamed("_id","new_id").show(10)
//    zips.select("_id","city","pop","state").filter(col("state") === "CA").orderBy(desc("pop")).show(10,false) // desc import org.apache.spark.sql.functions._
//
//    zips.createOrReplaceTempView("zips")
//    spark.sql("select _id,city,pop,state from zips order by pop desc limit 10").show(10,false)

    spark.close()


  }
}
