package com.ds.sql

import org.apache.spark.sql.SparkSession

object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import org.apache.spark.sql.functions._

    val df = spark.read.json("src/main/resources/data/student.json")

    df.groupBy("address").sum("age").show()
    df.groupBy("address").max("age").show()
    df.groupBy("address").min("age").show()
    df.groupBy("address").avg("age").show()
    df.groupBy("address").count().show()

    // select avg(age) as avg_age from df group by address having avg_age > 20
    df.groupBy("address")
      .avg("age")
      .withColumnRenamed("avg(age)", "avg_age")
      .where("avg_age > 20")
      .show()

    // agg
    df.groupBy("address").agg("age" -> "max", "age" -> "min").show()
    df.groupBy("address").agg(max("age"), min("age")).show()
  }
}
