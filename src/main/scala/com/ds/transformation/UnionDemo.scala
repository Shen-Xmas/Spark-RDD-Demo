package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object UnionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("filter Demo")
    val sc = new SparkContext(conf)
    val value1 = sc.makeRDD(Array(1, 2, 3, 4, 8, 9))
    val value2 = sc.makeRDD(Array(1, 2, 3, 5, 6, 7))
    value1.union(value2)
      .collect()
      .foreach(println)
  }
}
