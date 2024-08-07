package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndexDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("map partiotion with index demo")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5), 2)
      .mapPartitionsWithIndex {
        (partitionID ,iter) => { // 分区中的所有数据
          var result = List[Int]()
          var even = 0
          var odd = 0
          while (iter.hasNext) {
            val i = iter.next()
            if (i % 2 == 0) {
              even += i
              println(s"partition: ${partitionID} , num: ${i}")
            } else {
              odd += i
              println(s"partition: ${partitionID} , num: ${i}")
            }
          }
          result = result :+ even :+ odd
          result.iterator
        }
      }.collect().foreach(println)
  }
}
