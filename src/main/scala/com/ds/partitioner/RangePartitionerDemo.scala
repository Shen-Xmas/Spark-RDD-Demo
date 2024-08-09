package com.ds.partitioner

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object RangePartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("range partitioner demo")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array((1, "A"), (1, "B"), (2, "C"), (2, "D"), (2, "E"), (2, "BB"), (4, "CC"), (5, "DD"), (6, "EE")), 2)
    rdd.partitionBy(new RangePartitioner(3, rdd))
      .mapPartitionsWithIndex {
        (id, iter) => {
          println(s"id: ${id}  ,  data: " + iter.mkString(","))
          iter
        }
      }
      .collect()
  }
}
