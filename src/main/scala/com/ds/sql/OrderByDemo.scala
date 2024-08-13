package com.ds.sql

import org.apache.spark.sql.SparkSession

object OrderByDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.json("src/main/resources/data/student.json")

    df.sort("age").show()
    df.sort($"name").show()
    df.sort($"age".desc).show()
    df.sort(-$"age").show()
    df.sort("age", "name").show()

    // order by 底层就是sort
    df.orderBy()
  }
}
