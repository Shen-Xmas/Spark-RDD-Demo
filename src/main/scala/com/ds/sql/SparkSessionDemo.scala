package com.ds.sql

import org.apache.spark.sql.SparkSession

object SparkSessionDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
  }
}
