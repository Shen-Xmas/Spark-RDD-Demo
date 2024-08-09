package com.ds.persist

import org.apache.spark.{SparkConf, SparkContext}

class CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("check point demo").setMaster("local")
    val sc = new SparkContext(conf)
    val path = this.getClass.getResource("/data").getPath
    sc.setCheckpointDir(path)
    val path_data = this.getClass.getResource("/data/words.txt").getPath
    val rdd = sc.textFile(path_data)
      .flatMap(_.split(" "))
      .map((_, 1)) // 更改数据格式

    // check point
    rdd.checkpoint()

    // 触发第一次action操作 产生一个job
    println(rdd.countByKey().foreach(println))
    // 触发第二次action操作 产生第二个job
    println(rdd.countByKey().foreach(println))
  }
}
