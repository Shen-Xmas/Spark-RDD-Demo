package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("filter Demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array(1, 1, 1, 2, 2, 2, 2, 2, 3), 2)
      .distinct(3)
      .mapPartitionsWithIndex {
        (partitionId, data) => {
          println(s"分区 ${partitionId} 数据： "+ data.mkString(","))
          println("========================================")
          data
        }
      }
      .collect()
      .foreach(println)
  }
}
