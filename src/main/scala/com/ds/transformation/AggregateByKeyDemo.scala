package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce by key demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(("A", 9), ("B", 2), ("B", 8), ("B", 10), ("A", 6), ("A", 3)), 2)
      // 第一个zeroValue是初始值
      // 第二个参数有两个操作
      //    子操作1是分区内操作
      //    子操作2是分区间操作
      .aggregateByKey(20) ({
      //  初始值参与分区内的操作不参与分区间的操作
        (x, y) => {
          math.max(x, y)
        }
      }, {
        (x, y) => {
          x + y
        }
      })
      .collect()
      .foreach(println)
  }
}
