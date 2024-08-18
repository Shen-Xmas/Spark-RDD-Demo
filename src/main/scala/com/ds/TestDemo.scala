package com.ds

import org.apache.spark.{SparkConf, SparkContext}

object TestDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("save demo").setMaster("local")
    val sc = new SparkContext(conf)


    sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 5)
      // 生成的是文件夹 包含三部分 校验文件/是否写入成功文件/数据文件
      // 写入的位置如果文件已经存在 会报错
      // 一个分区对应一个文件
      // 可以使用repartition优化小文件 比如repartition(1) 只会产生一份数据文件
      .saveAsTextFile("src/main/resources/data/partitions")
  }
}
