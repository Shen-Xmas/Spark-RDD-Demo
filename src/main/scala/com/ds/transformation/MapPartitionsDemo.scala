package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    // local默认做1个分区
    val conf = new SparkConf().setMaster("local").setAppName("MapPartitions Demo")
    val context = new SparkContext(conf)
    context.parallelize(Array(1, 2, 3, 4, 5), 2)
      .mapPartitions{
        iter => {  // 分区中的所有数据
          var result = List[Int]()
          var even = 0
          var odd =0
          while (iter.hasNext) {
            val i = iter.next()
            if (i % 2 == 0) {
              even += i
              println(s"partition: ${TaskContext.getPartitionId()} , num: ${i}")
            } else {
              odd += i
              println(s"partition: ${TaskContext.getPartitionId()} , num: ${i}")
            }
          }
          result = result :+ even :+ odd
          result.iterator
        }
      }.collect().foreach(println)
  }
}
