package classes

import com.company.Framework.scalaclasses.Frame
import com.company.Framework.scalaclasses.RDDFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ColumnName,DataFrame,SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object Example extends App{
//  val df = new Frame().getData()
//  val column_name = Seq("ee","rr","id")
//  val df1 = df.select(column_name.map(c => col(c)): _*)
//    df1.show()
//  val rdd = new RDDFrame().getRDD()
//  rdd.take(10).foreach()
  val spark = SparkSession.builder().master("local[*]").appName("SparkExample").config("spark.driver.allowMultipleContexts","true").getOrCreate()
  import spark.implicits._
  val df = spark.read.format("csv").option("header", "true").load("/Users/xiangluo/Desktop/data/test.csv")
  df.show()
  val zip = udf((xs: Seq[String], ys: Seq[String]) => xs.zip(ys))


  val df1 = df.withColumn("new",explode(split(zip(col("name"), col("code")),"\\|")))
val df_null = df.filter(col("name").isNull || col("code").isNull)
  .withColumn("len",when(size(split(col("name"),"\\|")) >=1,lit(size(split(col("name"),"\\|")))))

  val df_null = df.filter(col("name").isNull || col("code").isNull)
      .withColumn("newname",split(col("name"),"\\|"))
      .withColumn("code",split(col("code"),"\\|")(0))


    df_null.show()


  val df_not_null = df.filter(col("name").isNotNull && col("code").isNotNull)
  df_not_null.show()
  val df1 = df_not_null.withColumn("newname",split(col("name"), "\\|")).drop("name")
    .withColumn("newcode",split(col("code"), "\\|")).drop("code")

  df1.show()
  val df2 = df1.withColumn("vars", explode(zip(col("newname"), col("newcode"))))
//
  val df3 = df2.select(col("p2p_flag"),col("tns_flag"),col("vars._1").alias("new_name"),col("vars._2").alias("new_code"))
      .filter(col("new_name") =!= "")
  df3.show()
//  df_null.union(df2).show()
  Thread.sleep(1000000)
}
