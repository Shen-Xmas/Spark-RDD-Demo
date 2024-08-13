package com.ds.sql

import org.apache.spark.sql.SparkSession

object FromFileToDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("read file")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

//    val schema = "name String, age Int, height Int"
    val csv = spark.read
              .options(Map(("delimiter", ","), ("header", "true"), ("inferschema", "true")))
//              .schema(schema)
              .csv("src/main/resources/data/student.csv")

    csv.printSchema()
    csv.show()
  }
}
