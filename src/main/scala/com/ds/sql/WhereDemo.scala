package com.ds.sql

import org.apache.spark.sql.SparkSession

object WhereDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.json("src/main/resources/data/student.json")

    df.filter("age > 30").show()
    df.filter("age > 30 and name == 'Tom'").show()

    // where底层调用的filter
    df.where("age > 30")
    df.where("age > 30 and name == 'Tom'")
  }
}
