package com.ds.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadSaveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.local.dir", "src/main/resources/sql")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 默认parquet  实际.parquet底层是这样
    val frame = spark.read.format("parquet").load("path/user.parquet")

    frame.select("name").write.mode(SaveMode.Overwrite).format("json").save("path") // 这里是一个目录

    // 内置支持的类型有  text csv json parquet ORC

    // text
    spark.read.text("path") // 返回DataFrame
    // 从文本中读取的话 就只有一个字段 字段名为value  字段值为每一行
    val df = spark.read.textFile("path") // 返回DataSet
      .map {
        line => {
          val split = line.split(",")
          (split(0), split(1))
        }
      }
      .toDF("col1Name", "col2Name")
    // [a, b, c]格式
    df.withColumn("col2Name", split(df("value"), ","))
      .selectExpr("split[0]", "split[1]")
      .toDF("col1Name", "col2Name")
    // select split[0] as stock, split[1] as price from (select split(value, ',') as split from table)

    // csv  默认分隔符为,  其他参数见文档
    spark.read.csv("path")
    spark.read.option("header", true).csv("path")  // 第一行作为字段名
    val schema =
      """
        |id int
        |name string
        |""".stripMargin
    spark.read.schema(schema).csv("path")

    // json
    // 强schema就是自己指定字段类型  弱schema靠json自己识别字段类型
    spark.read.json("path")
    spark.read.option("multiline", true).json("path") // 支持 json 的多行模式

    // parquet  列式存储格式  默认的 参数 spark.sql.sources.default
    // 自动推断分区  分区列字段类型仅支持自动推断出数字类型和字符串类型  禁止时统一为String 参数  spark.sql.sources.partitionColumnTypeInference.enabled true/false
    spark.read.parquet("path")

    // orc  spark sql  presto 等支持  但 impala不支持
    // spark 2.x 支持orc向量化读  参数 spark.sql.orc.impl=native  spark.sql.orc.enableVectorizedRead=true
    // 向量化读会成块(通常 1024 行组成 1 块)读入数据，而不是一次读一行。同时，操作会串起来，降低扫描、过滤、聚合、连接等集中操作时的 CPU 使用率
    // 对于用 SQL 命令 USING HIVE OPTIONS (fileFormat 'ORC') 创建的使用 Hive ORC SerDe(序列化和反序列化)的表，
    // Spark 会在配置参数 spark.sql.hive.convertMetastoreOrc 设为 true 时使用向量化读
    spark.read.orc("path")
    // 写两个分区
    spark.read.orc("path").write.orc("path/new/dt=20230813")
    spark.read.orc("path").write.orc("path/new/dt=20230814")
    // orc 也可以像parquet一样自动识别分区
    spark.read.orc("path").write.orc("path/new")

    // excel格式 需要加pom依赖
    spark.read.format("com.crealytics.spark.excel").load("path")
    import com.crealytics.spark.excel._
    spark.read.excel()
  }
}
