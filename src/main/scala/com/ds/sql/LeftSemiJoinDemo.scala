package com.ds.sql

import org.apache.spark.sql.SparkSession

object LeftSemiJoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val students = spark.read.json("src/main/resources/data/student.json")
    val classes = spark.read.json("src/main/resources/data/class.json")

    import spark.implicits._

    // 左半连接  只返回在左表存在的数据 没连接加列  就是类似in/exists的功能
    // 类似 select * from students where name in (select name from classes)
    students.join(classes, Seq("name"), "left_semi").show()
    students.join(classes, Seq("name"), "leftsemi").show()
    students.join(classes, Seq("name"), "semi").show()

    // 左反连接 left anti join  类似not in  和 left semi join 反着来
    students.join(classes, Seq("name"), "left_anti").show()
    students.join(classes, Seq("name"), "leftanti").show()
    students.join(classes, Seq("name"), "anti").show()

  }
}
