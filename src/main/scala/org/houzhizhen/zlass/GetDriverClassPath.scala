package org.houzhizhen.zlass

import org.apache.spark.sql.SparkSession

object GetDriverClassPath {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark GetJarsInDriver")
      .getOrCreate()
    val jars = getJavaClassPath(0)
    println(s"jars:" + jars)
    spark.stop()
  }
}
