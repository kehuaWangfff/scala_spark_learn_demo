package sparkRdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object demo2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("leranRdd").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    val testfile = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").load("/home/kehua/eclipse-workspace/scala_spark_learn_demo/dataset/input1")

    testfile.groupBy("id").pivot("tag").sum("value").show()
    testfile.printSchema()

  }
}