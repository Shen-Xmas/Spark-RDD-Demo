package com.ds.sql

import org.apache.spark.sql.SparkSession

object CollectionDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val list = List(("Jack", 28 ,184), ("Andy", 16 ,177), ("Bob", 42, 165))
    spark.createDataFrame(list)
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "age")
      .withColumnRenamed("_3", "height")
      .orderBy(desc("age"))
      .show()

    spark.createDataFrame(list)
      .toDF("name", "age", "height")
      .orderBy(desc("age"))
      .show()
  }
}
