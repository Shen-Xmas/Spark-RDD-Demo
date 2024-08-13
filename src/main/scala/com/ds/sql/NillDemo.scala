package com.ds.sql

import org.apache.spark.sql.SparkSession

object NillDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val classes = spark.read.json("src/main/resources/data/class.json")

    // 删除含有空值的行
    classes.na.drop().show()
    classes.na.drop(Array("name")).show()

    // 填充
    classes.na.fill("空的").show()
    classes.na.fill("空的", Array("name")).show()
    classes.na.fill(Map("name" -> "no name", "class" -> "NULL")).show()

    // 替换
    classes.na.replace(Array("name"), Map("Tom" -> "Cat"))   // 只有第一个map会生效  多个可以链式调用replace

    classes.where("name is null").show()
    classes.where($"name".isNull).show()
  }
}
