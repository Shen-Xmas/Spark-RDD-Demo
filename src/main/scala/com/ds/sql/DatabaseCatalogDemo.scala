package com.ds.sql

import org.apache.spark.sql.SparkSession

object DatabaseCatalogDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 引入catalog
    val catalog = spark.catalog

    // catalog 描述数据库  name   description  localtionUri

    spark.sql("show databases").show()
    catalog.listDatabases().show(false)

    // use database
    println(catalog.setCurrentDatabase("default"))

    println(catalog.getDatabase("default"))

    println(catalog.databaseExists("test"))
  }
}
