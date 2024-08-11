package com.ds.other

import org.apache.spark.{SparkConf, SparkContext}

object BrocastDemo {
  def main(args: Array[String]): Unit = {
    // 广播变量变量 executor  只可读 不可写
    val conf = new SparkConf().setMaster("local[*]").setAppName("brocast demo")
    val sc = new SparkContext(conf)

    var a = 2

    // 广播变量是只读变量
    val value1 = sc.broadcast(a)

    a = 10  // 广播变量广播出去以后更改没有广播出去
    // 广播变量是单独由Driver端发往Executor端，而不是在计算逻辑发往Executoe端时，闭包(closure)后一同发过去的

    sc.makeRDD(List(1 ,2, 3, 4, 5))
      .foreach(num => println(num * value1.value))

    val stu1 = new Student("ZhangSan", 18)
    val broV = sc.broadcast(stu1)
    // 这里的LiSi会生效  因为是引用地址   如果是stu1 = new Student() 就不会生效
    // 实际就是广播发送出去的是变量的地址
    stu1.name = "LiSi"
    sc.makeRDD(List(1 ,2, 3, 4, 5))
      .foreach(num => println(broV.value))
  }

  class Student(var name: String, var age: Int) extends Serializable {
    override def toString: String = s"name is ${name}, age is ${age}"
  }

}
