package classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("example").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
      .collect()
    wordCountsWithReduce.foreach(println)

    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()
    wordCountsWithGroup.foreach(println)
  }
}
