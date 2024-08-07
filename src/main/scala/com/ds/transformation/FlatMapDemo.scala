package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Map Demo")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("hello world", "hello scala", "heihei spark"))
      .flatMap(_.split(" "))
      .collect()
      .foreach(println)
  }
}
