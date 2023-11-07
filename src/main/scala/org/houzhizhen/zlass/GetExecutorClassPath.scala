package org.houzhizhen.zlass

import org.apache.spark.sql.SparkSession

object GetExecutorClassPath {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GetExecutorClassPath")
      .getOrCreate()
    val jars = spark.sparkContext.parallelize(1 to 1, 1)
      .map { i =>
      getJavaClassPath()
    }.collect().mkString(",")
    println(s"Driver jars:" + jars)
    spark.stop()
  }
}
