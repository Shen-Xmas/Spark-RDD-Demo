package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object CoalesceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("filter Demo")
    val sc = new SparkContext(conf)
    val value = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7), 3)
      .coalesce(5, true)
    println("重分区的个数： " + value.partitions.size)
    println("RDD依赖关系： " + value.toDebugString)
  }
}
