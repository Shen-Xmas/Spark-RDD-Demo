package com.ds.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

object DFDSRDDTransformationDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    val list = Array(("Jack", 28 ,184), ("Andy", 16 ,177), ("Bob", 42, 165))

    // 第一种 RDD to DF
    val value: RDD[Row] = sc.makeRDD(list)
      .map(x => Row(x._1, x._2, x._3))
    val scehma = new StructType()
      .add("name", "String", false)
      .add("age", "int", false)
      .add("height", "int", false)

    val df1 = spark.createDataFrame(value, scehma)
    df1.show()

    // 第二种 RDD to DF
    val df2 = spark.createDataFrame(list).toDF("name", "age", "height")
    df2.show()

    // 更省代码的
    val df3 = sc.makeRDD(list).toDF("name", "age", "height")
    df3.show()

    // RDD to DS
    val ds = sc.makeRDD(list)
      .map(x => Student(x._1, x._2, x._3))
      .toDS()
    ds.show()

    // DS to RDD
    ds.rdd

    // DF to RDD
    df1.rdd

    // DF to DS
    val dsT = df1.as("demo")

    // DS to DF
    val dfT = ds.toDF()
  }

  case class Student(name: String, age: Int, height: Int)

}
