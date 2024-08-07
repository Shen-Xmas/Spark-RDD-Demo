package com.ds.action

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object CustomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("count demo").setMaster("local")
    val sc = new SparkContext(conf)
    val value = sc.parallelize(Array(1, 9, 2, 3, 6, 4, 8, 5), 3)
      .filter(_ % 2 == 0)
    val result = sc.runJob(value, myAction _)
    result.foreach(println)
  }

  /*
  实现在driver端获取executor执行task返回的结果 比如task是个规则引擎 我想知道每条规则命中了几条数
   */
  def myAction(iterator: Iterator[Int]) = {
    var count = 0
    iterator.foreach {
      each => {
        count += 1
      }
    }
    (TaskContext.getPartitionId(), count)
  }

}


