package com.ds.sql

import org.apache.spark.sql.SparkSession

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val student = spark.read
      .options(Map(("delimiter", ","), ("header", "true"), ("inferschema", "true")))
      .csv("src/main/resources/data/student.csv")
    student.createTempView("student")

    // DSL
    student.cache()

    // sql
    // cache table 是即时生效的(eager)
    spark.sql("cache table student")
    // lazy 遇到action算子才会缓存
    spark.sql("cache lazy table student")
    spark.sql("select * from student").show()
    spark.sql("uncache table student")
  }
}
