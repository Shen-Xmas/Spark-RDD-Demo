package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sort by demo")
    val sc = new SparkContext(conf)
    val students = List(
      Student("Alice", 15),
      Student("Bob", 18),
      Student("Dave", 14),
      Student("Even", 21),
      Student("Frank", 28),
      Student("Chen", 9)
    )
    sc.makeRDD(students)
      // 降序排序 默认升序排序
//      .sortBy(_.age)(Ordering[Int].reverse, ClassTag.Int) // 防止泛型擦除
      .sortBy(-_.age)
      .collect()
      .foreach(println)
  }
}


case class Student(var name: String, var age: Int)