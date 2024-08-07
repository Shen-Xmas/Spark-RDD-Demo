package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object ZipDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("zip Demo")
    val sc = new SparkContext(conf)
    val value1 = sc.makeRDD(Array("A", "B", "C"))
    val value2 = sc.makeRDD(Array(1, 2, 3))
    value1.zip(value2)
      .collect()
      .foreach(println)
  }
}
