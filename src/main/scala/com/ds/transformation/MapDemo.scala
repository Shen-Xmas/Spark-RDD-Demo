package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}


object MapDemo {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Map Demo")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5))
      .map(_ * 3)
      .collect()
      .foreach(println)
  }
}
