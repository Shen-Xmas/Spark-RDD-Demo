package com.ds.action

import org.apache.spark.{SparkConf, SparkContext}

object SaveAsTextFileDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("save demo").setMaster("local")
    val sc = new SparkContext(conf)

    val path = this.getClass.getResource("/data/abc.text").getPath

    // 读
    // textFile可以读取一个目录下除了.和_开头外所有的文件 比如自己在目录下添加adb.text 也会被读取到
    // .和_开头的文件 会被认为不存在 指定路径也不行
    sc.textFile(path)
      .collect()
      .foreach(println)

    sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
      // 生成的是文件夹 包含三部分 校验文件/是否写入成功文件/数据文件
      // 写入的位置如果文件已经存在 会报错
      // 一个分区对应一个文件
      .repartition(1)  // 可以使用repartition优化小文件 比如repartition(1) 只会产生一份数据文件
      .saveAsTextFile("src/main/resources/data/test")

    // HDFS操作
    sc.textFile("hdfs://hadoop004:8020/data.test.txt")
      .collect()
      .foreach(println)

    // 这里之所以能够访问到 hdfs，是因为本地配置了 HADOOP_HOME，如下: // println(System.getenv("HADOOP_HOME"))
    // 如果本地没有配置 HADOOP_HOME，可以将 core-site.xml 和 hdfs - site.xml 放在 resources 目录下
    // 但是一旦放在 resourcer 目录下，这个时候默认的路径又变成了 hdfs:///， 本地的话需要加 file: ///

    // 端口关闭 传参 HDFS操作
    sc.textFile(args(0))
      .collect()
      .foreach(println)

  }
}
