package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sort by key demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array((3, "C"), (1, "A"), (4, "D"), (2, "B")), 1)
      // 分区前会打印两次
      .mapPartitionsWithIndex {
        (partitionId, data) => {
          var list = List[(Int, String)]()
          while (data.hasNext) {
            val next = data.next()
            list :+= next
            print(s"分区前 ${partitionId} 数据： " + next)
          }
          println()
          println("========================================")
          list.toIterator
        }
      }
      .sortByKey()
      .mapPartitionsWithIndex {
        (partitionId, data) => {
          var list = List[(Int, String)]()
          while (data.hasNext) {
            val next = data.next()
            list :+= next
            print(s"分区后 ${partitionId} 数据： " + next)
          }
          println()
          println("========================================")
          list.toIterator
        }
      }
      .collect()
      .foreach(println)
  }
}
