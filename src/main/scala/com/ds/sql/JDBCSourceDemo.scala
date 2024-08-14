package com.ds.sql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object JDBCSourceDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = "age string"

    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://ds-bigdata-001:3306/ds_test?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "ds_read")
      .option("password", "ds#readA0906")
      // 也可以不指定dbtable  使用"query", sql  自己写sql
      .option("dbtable", "stud")
      // 指定字段类型
      .option("customSchema", schema)
      .load()
    reader.printSchema()

    var properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "ds_read")
    properties.put("password", "ds#readA0906")
    spark.read.jdbc("jdbc:mysql://ds-bigdata-001:3306/ds_test?useSSL=false", "stud", properties)
  }
}
