package com.company.Framework.scalaclasses

import org.apache.spark.sql.{ColumnName,DataFrame,SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


class Frame {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("SparkExample")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
}
