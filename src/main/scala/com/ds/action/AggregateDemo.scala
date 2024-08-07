package com.ds.action

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("count demo").setMaster("local")
    val sc = new SparkContext(conf)
    val i = sc.parallelize(Array(1, 2, 3, 9, 4, 5), 3)
      .aggregate(10)(
        {
          (x, y) => {
            println(s"partition id is ${TaskContext.getPartitionId()} and x is ${x}  y is ${y}")
            x + y
          }
        },
        {
          (a, b) => {
            println(s"partition id is ${TaskContext.getPartitionId()} and a is ${a}  b is ${b}")
            a + b
          }
        }
      )
    println(i)
  }
}

//partition id is 0 and x is 10  y is 1
//partition id is 0 and x is 11  y is 2

//partition id is 0 and a is 10  b is 13

//partition id is 1 and x is 10  y is 3
//partition id is 1 and x is 13  y is 9

//partition id is 0 and a is 23  b is 22

//partition id is 2 and x is 10  y is 4
//partition id is 2 and x is 14  y is 5

//partition id is 0 and a is 45  b is 19

//64