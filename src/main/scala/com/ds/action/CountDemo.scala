package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object CountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("count demo").setMaster("local")
    val sc = new SparkContext(conf)
    val cnt = sc.parallelize(Array(1, 2, 3, 4, 5), 3)
      .count()
    println(cnt)
  }
}
