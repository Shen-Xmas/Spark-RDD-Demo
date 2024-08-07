package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object TakeDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("count demo").setMaster("local")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5), 5)
      .takeSample(true, 3)
      .foreach(println)
  }
}
