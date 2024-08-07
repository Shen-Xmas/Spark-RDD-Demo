package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object TakeOrderedDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("count demo").setMaster("local")
    val sc = new SparkContext(conf)

//    sc.parallelize(Array(2, 1, 3, 5, 4), 5)
//      .takeOrdered(2)
//      .foreach(println)

    var list = List(
      Girl("Alice", 12, 56)
      , Girl("Lily", 19, 50)
      , Girl("July", 23, 62)
      , Girl("Nana", 28, 48)
      , Girl("Mary", 11, 39)
    )

//    // Ordering.by
//    sc.makeRDD(list)
//      // Ordering.by  by[T, S](f: T => S) 比如这里用名字比较 就是Girl类型变成String去比较  ()里就是怎么把Girl类型变成想要的String
//      .takeOrdered(2)(Ordering.by[Girl, Int](_.age))
//      .foreach(println)

    // fromLessThan
    sc.makeRDD(list)
      .takeOrdered(2)(Ordering.fromLessThan[Girl](_.age > _.age))
      .foreach(println)

  }

//  case class Girl(name: String, age: Int, weight:Int) {
//    override def toString: String = s"name is ${name}, age is ${age}, weight is ${weight}"
//  }
  // 自定义排序的写法
  case class Girl(name: String, age: Int, weight:Int) {
    override def toString: String = s"name is ${name}, age is ${age}, weight is ${weight}"
  }

}
