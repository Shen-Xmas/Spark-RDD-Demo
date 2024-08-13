package com.ds.sql

import org.apache.spark.sql.SparkSession

object LimitUnionDistinctDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read
      .options(Map(("delimiter", ","), ("header", "true"), ("inferschema", "true")))
      .csv("src/main/resources/data/student.csv")

    df.limit(2).show()
    df.distinct().show()

    //  合并 不去重
    df.union(df).show()

    // 根据指定字段去重  只会留取第一次出现的 后面的都会被抛弃
    df.dropDuplicates("name").show()

    // 统计字段的信息 多少条数据有这个字段 平均值 标准差  最小值 最大值
    df.describe("age").show()
    df.describe().show()
  }
}
