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
      , Girl("Nana", 19, 48)
      , Girl("Mary", 19, 48)
    )

//    // Ordering.by
//    sc.makeRDD(list)
//      // Ordering.by  by[T, S](f: T => S) 比如这里用名字比较 就是Girl类型变成String去比较  ()里就是怎么把Girl类型变成想要的String
//      .takeOrdered(2)(Ordering.by[Girl, Int](_.age))
//      .foreach(println)

//    // fromLessThan
//    sc.makeRDD(list)
//      .takeOrdered(2)(Ordering.fromLessThan[Girl](_.age > _.age))
//      .foreach(println)

    // 自定义比较 1  因为Girl类型编写后本身可排序了 不用传其他参数
//    sc.makeRDD(list)
//      .takeOrdered(5)
//      .foreach(println)

    // 自定义比较 2  因为Girl类型编写后本身可排序了 不用传其他参数
    sc.makeRDD(list)
      .takeOrdered(5)(Ordering.by(customGirlOrdered))
      .foreach(println)

  }


  case class Girl(name: String, age: Int, weight:Int) {
    override def toString: String = s"name is ${name}, age is ${age}, weight is ${weight}"
  }

//  // 自定义排序的写法 1
//  case class Girl(name: String, age: Int, weight:Int) extends Ordered[Girl] {
//    // 例如 先比较年龄 再比较体重 最后比较名字
//    override def compare(that: Girl): Int = {
//      var result = this.age - that.age
//      if(result == 0) {
//        result = this.weight - that.weight
//        if(result == 0) {
//          result = this.name.compareTo(that.name)
//        }
//      }
//      result
//    }
//
//    override def toString: String = s"name is ${name}, age is ${age}, weight is ${weight}"
//  }

  // 自定义排序的写法 2
  def customGirlOrdered(girl: Girl) = (girl.age, girl.weight, girl.name)

}
