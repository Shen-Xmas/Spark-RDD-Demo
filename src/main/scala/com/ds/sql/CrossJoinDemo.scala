package com.ds.sql

import org.apache.spark.sql.SparkSession

object CrossJoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val students = spark.read.json("src/main/resources/data/student.json")
    val classes = spark.read.json("src/main/resources/data/class.json")

    // 就是笛卡尔积
    students.crossJoin(classes).show(10)
  }
}
