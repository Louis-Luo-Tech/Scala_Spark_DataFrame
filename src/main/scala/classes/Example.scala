package classes

import org.apache.spark.sql.SparkSession

object Example extends App{
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("SparkExample")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


    

    spark.stop()
}
