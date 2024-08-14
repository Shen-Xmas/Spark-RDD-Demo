package com.ds.sql

import org.apache.spark.sql.SparkSession

object ColumnCatalogDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    val catalog = spark.catalog

    // catalog 描述列  name  description  dataType  nullable   isPartition   isBucket

    val df = spark.read
      .options(Map(("delimiter", ","), ("header", "true"), ("inferschema", "true")))
      .csv("src/main/resources/data/student.csv")

    df.createTempView("tmp_view")

    catalog.listColumns("tmp_view").show(false)
  }
}
