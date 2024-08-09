package com.ds.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object CustomPartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("custom partitioner demo")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array((1, "A"), (10, "B"), (8, "C"), (8, "D"), (6, "E"), (2, "BB"), (4, "CC"), (5, "DD"), (6, "EE")), 2)
    rdd.partitionBy(new MyPartitioner(3))
      .mapPartitionsWithIndex {
        (id, iter) => {
          println(s"id: ${id}  ,  data: " + iter.mkString(","))
          iter
        }
      }
      .collect()
  }
}

class MyPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case 1 => 0
      case 2 => 1
      case _ => 2
    }
  }
}