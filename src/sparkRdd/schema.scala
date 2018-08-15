package sparkRdd

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.rdd._
object schema {
  case class Person(name:String,age:String,job:String,level:String)

  def main(args: Array[String]) {
    val sqlContext = SparkSession.builder().appName("Spark-2.0-SQL-APP-1").master("local")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()
    import sqlContext.implicits._
   sqlContext.sparkContext.setLogLevel("ERROR")
    val fileRdd = sqlContext.sparkContext.textFile("/home/kehua/eclipse-workspace/scala_spark_learn_demo/dataset/people", 2)
   // println(fileRdd.collect().mkString("\n"))
    val splitfile = fileRdd.map(_.split(","))
      .map(line => Person(line(0), line(1), line(2), line(3)))
    //  val length =splitfile.map(str=>str.length)
      //println(length.collect().mkString("  "))
   splitfile.toDF().show()
      //splitfile.show()
   // println(splitfile.take(1).mkString(""))
//    val struct = StructType(Array(
//      StructField("name", StringType, true),
//      StructField("age", IntegerType, true), StructField("job", StringType, true), 
//      StructField("level", StringType, true), StructField("sales", IntegerType, true)))
//    //val df = sqlContext.createDataFrame(splitfile, struct)
   // df.show()
   // df.printSchema()
  }

}