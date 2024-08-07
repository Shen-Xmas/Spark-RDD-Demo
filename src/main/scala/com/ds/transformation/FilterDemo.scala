package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object FilterDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("filter Demo")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("aa", "ab", "ac", "bc"))
      .filter(_.startsWith("a"))
      .collect()
      .foreach(println)
  }
}
