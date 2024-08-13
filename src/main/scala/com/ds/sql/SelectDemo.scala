package com.ds.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object SelectDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.json("src/main/resources/data/student.json")

    // select   ''   ""   $""   col("")   df("")对象本身的引用 都可以
    df.select("name", "age").show()

    // 需要表达式的时候
    df.select($"age" + 10, $"age")
    df.select('age + 10, 'name)

    df.select(expr("age + 10"))
    df.selectExpr("power(age, 2), name")
    df.selectExpr("round(age, -1) as newAge")
    df.select($"age".cast(StringType))

    // drop 删除指定列  返回一个新DF  原有不变
    df.drop("height")

    // withColumn 修改列值
    df.withColumn("age", $"age" + 10)

  }
}
