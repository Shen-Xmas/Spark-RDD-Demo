package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object CountByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce by key demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(("A", 1), ("B", 2), ("B", 10), ("A", 6), ("A", 2)), 2)
      .countByKey()
      .foreach(println)
  }
}
