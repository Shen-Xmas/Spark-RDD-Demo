package com.ds.sql

import org.apache.spark.sql.SparkSession

object FunctionCatalogDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val catalog = spark.catalog

    // catalog 描述函数  name  database  description className isTemporary  函数和数据库绑定

    catalog.listFunctions().show(100, false)

    catalog.listFunctions("default").show()

    println(catalog.getFunction("abs"))

    println(catalog.functionExists("abss"))

  }
}
