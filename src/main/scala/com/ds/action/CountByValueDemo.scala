package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object CountByValueDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce by key demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(("A", 1), ("B", 1), ("B", 6), ("A", 6), ("A", 1)), 2)
      .countByValue()
      .foreach(println)
  }
}
