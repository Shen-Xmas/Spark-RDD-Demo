package com.ds.action

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("collect demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.parallelize(Array((1, "A"), (2, "B"), (3, "C"), (2, "D"), (3, "E")), 3)
      .foreach(println)

    // 累加器
    val value = sc.parallelize(Array(1, 2, 3, 4, 5))
    val sum: LongAccumulator = sc.longAccumulator("sum")
    value.foreach {
      num => {
        sum.add(num)
      }
    }
    println(s"sum is ${sum.value}")
  }
}
