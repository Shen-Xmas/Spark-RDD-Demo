package com.ds.transformation

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

object CustomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custom Transformation").setMaster("local")
    val sc = new SparkContext(conf)
    val inputData: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    new MyTransformationRDD(inputData)
      .collect()
      .foreach(println)
  }
}

class MyTransformationRDD(prev: RDD[Int]) extends RDD[Int](prev) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    val ints = firstParent[Int].iterator(split, context)
    ints.map(_*3)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[Int].partitions
  }
}
