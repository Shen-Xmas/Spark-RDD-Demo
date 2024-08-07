package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object MapValuesDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("zip Demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array((1, "A"), (2, "B"), (3, "C")))
      .mapValues("**" + _ + "**")
      .collect()
      .foreach(println)
  }
}
