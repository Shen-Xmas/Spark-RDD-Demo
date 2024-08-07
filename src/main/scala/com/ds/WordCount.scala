package com.ds

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("text file").setMaster("local")
    val sc = new SparkContext(conf)
    val path = this.getClass.getResource("/data/words.txt").getPath
    sc.textFile(path)
      .flatMap(_.split(" "))
      .map((_, 1))  // 更改数据格式
      .countByKey()
      .foreach(println)
  }
}
