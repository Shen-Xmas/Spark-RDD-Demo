package com.ds.sql

import org.apache.spark.sql.SparkSession

object ViewDemo {
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

    // 创建视图
    // 1 DSL
    student.createTempView("student_tmp_view")  // 临时视图只存在于这个session
    student.createGlobalTempView("student_view")  // 永久视图切换session也存在
    spark.sql("select * from student_tmp_view").show()
    // 使用DF创建的全局视图 一定要加 global_tmp.
    spark.sql("select * from global_tmp.student_view").show()

    // 2 sql
    spark.sql("create temporary view tmp_view2(name, age) as values('Jack', 18) ")
    spark.sql("create view view2(name, age) as values('Jack', 18) ")
    spark.sql("select * from tmp_view2")
    // sql的写法 全局视图不需要加 global_tmp.
    spark.sql("select * from view2")

  }
}
