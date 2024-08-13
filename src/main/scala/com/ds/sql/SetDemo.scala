package com.ds.sql

import org.apache.spark.sql.SparkSession

object SetDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val students = spark.read.json("src/main/resources/data/student.json")
    val students1 = spark.read.json("src/main/resources/data/student1.json")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val dfName = students.select("name")
    val dfName1 = students1.select("name")

    // 并集
    dfName.union(dfName1)
    // 并集  调用的就是union  没有区别
    dfName.unionAll(dfName1)
    // 交集
    dfName.intersect(dfName1)
    // 差集
    dfName.except(dfName1)
  }
}
