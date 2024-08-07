package com.ds.transformation

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce by key demo")
    val sc = new SparkContext(conf)
    val list = List(("female", "Alice"), ("male", "Bob"), ("female", "Lily"), ("male", "Dave"), ("male", "Jack"), ("male", "Sam"))
    // ("male", (List("Bob", "Dave", "Jack", "Sam"), 4))
    // ("female", (List("Alice", "Lily"), 2))
    // 本来的数据结构 (String, String)
    // 想要的数据结构 (String, (List[String], Int))

    // combineByKey的参数
    // 第一个 createCombiner: V => C 将已知的value类型转换为目标value类型  关系映射[组合器函数]
            // ("female", "Alice") ==> ("female", (List("Alice"), 1))
            // ("male", "Bob") ==> ("male", (List("Bob"), 1))
            // 第一次遇到新key 都转换成这个样子的数据
            // 当遇到已知的key 不会转换 直接就进入第二个参数做分区内合并
    // 第二个 mergeValue: (C, V) => C 分区内聚合
            // 第一个参数相当于上一步创建的结果类型 第二个参数是相同key的value值
    // 第三个 mergeCombiners: (C, C) => C 分区间聚合
            // 第一个分区 ("male", (List("Bob"), 1))
            // 第二个分区 ("male", (List("Dave", "Jack", "Sam"), 3))

    // 第四个第五个可有可无  partitioner 指定分区函数默认Hash   mapSideCombine 是否在map端进行combine 默认true

    // combine处理的都是value
    sc.makeRDD(list, 2)
      .combineByKey(
        (x: String) => (List(x), 1),
        // people: (List[String], Int)相当于上一步创建的结果类型 x: String是相同key的value值
        // :: 不管x是不是列表 都是people._1这个列表的第一个元素 // ::可以用于pattern match ，而+:则不行.
        (people: (List[String], Int), x: String) => (x :: people._1, people._2 + 1),
        (people_par1: (List[String], Int), people_par2: (List[String], Int)) =>
          (people_par1._1 ::: people_par2._1, people_par1._2 + people_par2._2)
      )
      .collect()
      .foreach(println)
    // 结果
    // (female,(List(Lily, Alice),2))
    //(male,(List(Bob, Sam, Jack, Dave),4))
  }
}
