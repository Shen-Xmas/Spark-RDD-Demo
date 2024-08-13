package com.ds.sql

import org.apache.spark.sql.SparkSession

object RangeDSDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("range")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import org.apache.spark.sql.functions._

    val value = spark.range(5, 100, 5)
    value.orderBy(desc("id")).show(5)
    value.describe().show()
    value.printSchema()

    println(value.rdd.map(_.toInt).stats())
    println(value.rdd.getNumPartitions)
  }
}
