package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("cogroup demo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(("A", 1), ("B", 2), ("B", 10), ("C", 6), ("A", 2)), 2)
    val rdd2 = sc.makeRDD(List(("A", 2), ("B", 3), ("B", 11), ("D", 7), ("A", 3)), 3)
    val result = rdd1.join(rdd2)
    result.collect().foreach(println)

    // 结果
    //(B,(2,3))
    //(B,(2,11))
    //(B,(10,3))
    //(B,(10,11))
    //(A,(1,2))
    //(A,(1,3))
    //(A,(2,2))
    //(A,(2,3))

    println("rdd1 partitions: " + rdd1.partitions.length)
    println("rdd2 partitions: " + rdd2.partitions.length)
    println("result partitions: " + result.partitions.length)
  }
}
