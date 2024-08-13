package com.ds.sql

import org.apache.spark.sql.SparkSession

object LeftJoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val students = spark.read.json("src/main/resources/data/student.json")
    val classes = spark.read.json("src/main/resources/data/class.json")

    // LEFT OUTER JOIN 等价 LEFT JOIN
    // 指定 joinType 就要用 Seq 字段
    students.join(classes, Seq("name"), "left_outer").show()
    students.join(classes, Seq("name"), "leftouter").show()
    students.join(classes, Seq("name"), "left").show()

    import spark.implicits._

    // 如果两张表字段名不对应
    students.join(classes, $"name" === $"class", "left").show()

    // 右外连接一样
    students.join(classes, Seq("name"), "right_outer").show()
    students.join(classes, Seq("name"), "rightouter").show()
    students.join(classes, Seq("name"), "right").show()

    // 全外连接
    students.join(classes, Seq("name"), "full_outer").show()
    students.join(classes, Seq("name"), "fullouter").show()
    students.join(classes, Seq("name"), "outer").show()
    students.join(classes, Seq("name"), "full").show()
  }
}
