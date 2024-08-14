package com.ds.sql

import org.apache.spark.sql.SparkSession

object MysqlToHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      // 开启动态分区
      .config("hive.exec.dynamic.partition", true)
      // 开启非严格模式
      .config("hive.exec.dynamic.partition.mode", "nostrict")
      .getOrCreate()

    // mysql数据源
    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://ds-bigdata-001:3306/ds_test?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "ds_read")
      .option("password", "ds#readA0906")
      // 也可以不指定dbtable  使用"query", sql  自己写sql
      .option("dbtable", "stud")
      .load()

    // 创建一个临时视图
    reader.createTempView("stud")

    // hive先创建动态分区表
    spark.sql("create table ds_spark.stud(name String) partitioned by(age int)")

    // 往动态分区表导入数据
    spark.sql("insert overwrite table ds_spark.stud partition(age) select name,age from stud")
  }
}
