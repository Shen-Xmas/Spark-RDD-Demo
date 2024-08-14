package com.ds.sql

import org.apache.spark.sql.SparkSession

object ManagerTableDemo {
  def main(args: Array[String]): Unit = {

    // EXTERNAL 外部表 无管理表
    // MANAGED 内部表 管理表
    // VIEW  视图

    val spark = SparkSession.builder()
      .master("local")
      .appName("table test")
      .config("spark.local.dir", "src/main/resources/data")
      .getOrCreate()

    import spark.implicits._

    // 创建数据库
    // spark-warehouse 下 多了一个ds_test.db的目录
    spark.sql("create database ds_test")
    spark.sql("show databases").show()
    spark.sql("use ds_test")

    Thread.sleep(5000)

    // 内部表
    // 创建表
    // spark-warehouse 下 ds_test.db的目录下多了一个test目录
    spark.sql(
      """
        |create table `ds_test`.`test`
        |(
        | id Int,
        | name String
        |)
        |Using csv
        |""".stripMargin)

    spark.sql("show tables").show()

    Thread.sleep(5000)

    // 新增数据
    // spark-warehouse 下 ds_test.db的目录下test目录 首先有一个短暂的_temporary目录 成功插入后有分区数据文件和_SUCCESS等文件
    spark.sql(
      """
        |insert into ds_test.test values(1, "Alice")
        |""".stripMargin)

    spark.table("ds_test.test").show()

    Thread.sleep(10000)

    // 删除表
    // spark-warehouse ds_test.db的目录下test目录被删除
    spark.sql(
      """
        |drop table ds_test.test
        |""".stripMargin)

    spark.sql("show tables").show()

    Thread.sleep(10000)

    // 删除数据库
    // spark-warehouse 下 ds_test.db目录被删除
    spark.sql(
      """
        |drop database ds_test
        |""".stripMargin)

    spark.sql("show databases").show()

    Thread.sleep(10000)
  }
}
