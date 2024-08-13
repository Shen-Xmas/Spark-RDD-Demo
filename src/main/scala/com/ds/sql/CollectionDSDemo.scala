package com.ds.sql

import org.apache.spark.sql.SparkSession

object CollectionDSDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._  // 将JVM中的类型转换成spark的类型
    import org.apache.spark.sql.functions._

    val list = List(Student("Jack", 28 ,184), Student("Andy", 16 ,177), Student("Bob", 42, 165))
    val value = spark.createDataset(list)
    value.printSchema()
    value.show()
  }

  case class Student(name: String, age: Int, height: Int)

}
