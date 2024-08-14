package com.ds.sql

import org.apache.spark.sql.SparkSession

object ExternalTableDemo {
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

    // spark-warehouse目录下 新增ds_test.db目录
    spark.sql("create database ds_test")
    spark.sql("show databases").show()
    spark.sql("use ds_test")

    Thread.sleep(5000)

    // 外部表
    // spark-warehouse目录下 ds_test.db目录无变化  并不会新增表名目录
    // PATH需要是目录 会读取目录下全部非. _开头文件
    // 如果找不到这个路径 会转为内部表在spark-warehouse目录下新建这个路径
    spark.sql(
      """
        |create table ds_test.please
        |(
        | id Int,
        | name String
        | )
        | Using csv
        | OPTIONS (
        | PATH 'file:///Users/xmas/MyProjects/spark-new/spark_new/src/main/resources/data/test'
        | )
        | """.stripMargin)

    spark.sql("show tables").show()
    spark.table("ds_test.please").show()

    Thread.sleep(10000)

    // insert overwrite 会覆写文件  如果该目录下有多个文件 会清空目录再写
    // insert into 每次插入会新生成一个文件
    // 删除表和库  这个目录下的数据不会删除
    spark.sql(
      """
        |insert into ds_test.please values(10, "Jack")
        |""".stripMargin)

    spark.table("ds_test.please").show()

    Thread.sleep(10000)

    spark.sql(
      """
        |drop table ds_test.please
        |""".stripMargin)

    spark.sql("show tables").show()

    Thread.sleep(5000)

    spark.sql(
      """
        |drop database ds_test
        |""".stripMargin)

    spark.sql("show databases").show()

    Thread.sleep(10000)
  }
}
