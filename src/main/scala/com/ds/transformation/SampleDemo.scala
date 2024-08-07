package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object SampleDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("filter Demo")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7))
      .sample(false, 0.2)
      .collect()
      .foreach(println)
  }
}
