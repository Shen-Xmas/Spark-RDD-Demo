package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("glom demo")
    val sc = new SparkContext(conf)
    // 结果 Array(Array(1, 2), Array(3, 4, 5))
    sc.parallelize(Array(1, 2, 3, 4, 5),2)
      .glom()
      .collect()
      .foreach {
        arr => {
          arr.foreach(print)
          println()
        }
      }
  }
}
