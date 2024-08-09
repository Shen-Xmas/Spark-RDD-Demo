package com.ds.partitioner

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object HashPartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("hash partitioner demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"), (2, "BB"), (3, "CC"), (6, "DD"), (8, "EE")), 2)
      .partitionBy(new HashPartitioner(3))
      .mapPartitionsWithIndex {
        (id, iter) => {
          println(s"id: ${id}  ,  data: " + iter.mkString(","))
          iter
        }
      }
      .collect()
  }
}
