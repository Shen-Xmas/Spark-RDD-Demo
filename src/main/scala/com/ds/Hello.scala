package com.ds

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("text file")
    val sc = new SparkContext(conf)
    val value : RDD[String] = sc.textFile(args(0), 5)
    value.collect.foreach(x => println(x))
  }
}
