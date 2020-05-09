name := "Scala_Spark_DataFrame"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
//  "mysql" % "mysql-connector-java" % "5.1.6"
)

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.19"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.10.0"

libraryDependencies += "org.apache.flink" % "flink-core" % "1.10.0"

libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.10.0" % "provided"

libraryDependencies += "org.apache.flink" % "flink-table" % "1.10.0" % "provided" pomOnly()

libraryDependencies += "com.typesafe" % "config" % "1.3.3"