package com.louis.bigdata

import org.apache.spark.sql.SparkSession

object HiveSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("HiveSourceApp")
      //      .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
      .enableHiveSupport() //this is important
      .getOrCreate()
    spark.table("pk").show()
    spark.close()
  }

}
//hdfs://localhost:9000/user/hive/warehouse