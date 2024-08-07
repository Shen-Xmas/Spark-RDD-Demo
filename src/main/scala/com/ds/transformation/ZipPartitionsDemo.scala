package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object ZipPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("zip Demo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array("A", "B", "C"), 2)
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5), 2)

    rdd1.zipPartitions(rdd2) {
      (iter1, iter2) => {
        var result = List[String]()
        while (iter1.hasNext || iter2.hasNext) {
          if(iter1.hasNext && !iter2.hasNext) {
            result ::= (iter1.next() + "===" + 0)
          } else if (!iter1.hasNext && iter2.hasNext) {
            result ::= ("X" + "===" + iter2.next())
          } else {
            result ::= (iter1.next() + "===" + iter2.next())
          }
        }
        result.iterator
      }
    }
    .collect()
    .foreach(println)

  }
}
