package com.ds.other

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    // 累加器变量  driver端可读  executor可写 只能累加 不可读
    val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("broadcastTest"))
    // Spark 提供了三种累加器
    // 对于整型数值累加就采用 longAccumulator
    // 浮点型数值累加采用 doubleAccumulator(整型数值都可以转化成浮 点型数值 所以 doubleAccumulator 也可以进行整型数值的累加)
    // collectionAccumulator 的累加操作是将目标元素放入一个集合中

    // longAccumulator
//    val oddCnt = sc.longAccumulator("oddCnt")
//    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 7, 9, 10, 11))
//    rdd.foreach(number => if (number % 2 == 1) oddCnt.add(1))
//    println("奇数的数量是:" + oddCnt.value)

    // collectionAccumulator
    val map = sc.collectionAccumulator[String]("map")
    val rdd = sc.makeRDD(List("hello", "world", "hi", "scala", "hallo", "spark"))
    rdd.foreach(e => if (e.startsWith("h")) map.add(e))
    println (map.value)
  }
}
