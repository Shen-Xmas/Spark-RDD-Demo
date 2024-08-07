package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object CogroupDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cogroup demo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(("A", 1), ("B", 2), ("B", 10), ("A", 6), ("A", 2)), 2)
    val rdd2 = sc.makeRDD(List(("A", 2), ("B", 3), ("B", 11), ("A", 7), ("A", 3)), 2)
    rdd1.cogroup(rdd2)
      .collect()
      .foreach(println)
  }
}
