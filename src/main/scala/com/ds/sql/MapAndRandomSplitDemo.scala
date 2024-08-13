package com.ds.sql

import org.apache.spark.sql.SparkSession

object MapAndRandomSplitDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val df = spark.read
      .options(Map(("delimiter", ","), ("header", "true"), ("inferschema", "true")))
      .csv("src/main/resources/data/student.csv")

//    // map
//    // getAs获取的字段 统一的字段名为value
//    df.map(_.getAs[String]("name"))
//      .withColumnRenamed("统一的字段名为value", "Name")
//      .show()

    // randomSplit 随机切分
    val array = df.randomSplit(Array(0.3, 0.5, 0.8))
    println(array(0).count())
    println(array(1).count())
    println(array(2).count())
  }
}
