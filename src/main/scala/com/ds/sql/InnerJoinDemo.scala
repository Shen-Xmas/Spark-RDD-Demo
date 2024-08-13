package com.ds.sql

import org.apache.spark.sql.SparkSession


object InnerJoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val students = spark.read.json("src/main/resources/data/student.json")
    val classes = spark.read.json("src/main/resources/data/class.json")

    students.join(classes, "name").show()
  }
}
