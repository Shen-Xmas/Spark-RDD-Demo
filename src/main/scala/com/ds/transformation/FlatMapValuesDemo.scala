package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapValuesDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("zip Demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array((1, "NAME A"), (2, "ADDRESS B"), (3, "PHONENUM C")))
      .flatMapValues(_.split(" "))
      .collect()
      .foreach(println)
  }
}
