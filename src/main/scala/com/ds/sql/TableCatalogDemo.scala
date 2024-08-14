package com.ds.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object TableCatalogDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    // 引入catalog
    val catalog = spark.catalog

    // catalog 描述表  name  database  description  tableType [table/view]  isTemporary

    val schema = new StructType()
      .add("id", IntegerType, false)
      .add("name", StringType, false)
    val option = new java.util.HashMap[String, String]()
    catalog.createTable("ds_table", "csv", schema, "desc: test table", option)

    // show tables
    catalog.listTables().show(false)

    println(catalog.getTable("ds_table"))
    println(catalog.tableExists("ds_table1"))

    val df = spark.read
            .options(Map(("delimiter", ","), ("header", "true"), ("inferschema", "true")))
            .csv("src/main/resources/data/student.csv")

    df.createTempView("tmp_view")
    catalog.listTables().show(false)
    // 删除视图
    catalog.dropTempView("tmp_view")
    catalog.listTables().show(false)

    // 缓存
    catalog.cacheTable("ds_table")
    // 判断是否缓存
    catalog.isCached("ds_table")
    // 卸载缓存
    catalog.uncacheTable("ds_table")
    // 清除所有缓存
    catalog.clearCache()
    // 刷新表
    catalog.refreshTable("ds_table")


  }
}
