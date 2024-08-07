package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("count demo").setMaster("local")
    val sc = new SparkContext(conf)
    val i = sc.parallelize(Array(1, 2, 3, 9, 4, 5), 3)
      .filter(_%2 == 0)
      .reduce(math.max)
    println(i)
  }
}
