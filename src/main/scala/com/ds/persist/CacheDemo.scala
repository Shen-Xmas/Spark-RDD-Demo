package com.ds.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("text file").setMaster("local")
    val sc = new SparkContext(conf)
    val path = this.getClass.getResource("/data/words.txt").getPath
    val rdd = sc.textFile(path)
      .flatMap(_.split(" "))
      .map((_, 1)) // 更改数据格式
      .persist(StorageLevel.MEMORY_ONLY)
    // 触发第一次action操作 产生一个job
    println(rdd.countByKey().foreach(println))
    // 触发第二次action操作 产生第二个job
    println(rdd.countByKey().foreach(println))
  }
}
