package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object CollectDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("collect demo").setMaster("local")
    val sc = new SparkContext(conf)
    sc.parallelize(Array((1, "A"), (2, "B"), (3, "C"), (2, "D"), (3, "E")), 2)
//      .collectAsMap()
      .collect()
      .foreach(println)
  }
}
