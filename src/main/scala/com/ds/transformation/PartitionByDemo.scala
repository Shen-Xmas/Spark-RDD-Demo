package com.ds.transformation

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("partition by demo")
    val sc = new SparkContext(conf)
    sc.makeRDD(Array((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")), 2)
      .partitionBy(new HashPartitioner(3))
//      .repartition(3)
      .mapPartitionsWithIndex {
        (id, iter) => {
          println(s"id: ${id}  ,  data: " + iter.mkString(","))
          iter
        }
      }
      .collect()
  }
}
