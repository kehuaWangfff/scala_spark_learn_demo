package sparkRdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object demo1 {
  def main(srgs: Array[String]) {
    val conf = new SparkConf().setAppName("leranRdd").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val num = Array(1, 2, 3, 5, 6, 7, 8, 9, 4)
    val numRDD = sc.parallelize(num, 2)
    val sum = numRDD.reduce(_ + _)
    println("rdd累加求和等于  " + sum)

    //通过localfile创建JAVARDD
    val localRdd = sc.textFile("/home/kehua/eclipse-workspace/scala_spark_learn_demo/dataset/function设计", 3)
    val count = localRdd.map(line => line.length()).reduce(_ + _)
    println("该文本的字数是  " + count)
  }
}