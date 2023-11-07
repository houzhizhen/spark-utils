package org.houzhizhen.zlass

import org.apache.spark.sql.SparkSession

object UploadFilesDriverClasspath {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: UploadFilesDriverClasspath <file>")
      println("but got para " + args.mkString(","))
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("Spark GetJarsInDriver")
      .getOrCreate()
    val path = args(0);
    val jars = uploadFilesInClassPath(path, spark.sessionState.newHadoopConf());
    println(s"Driver uploaded files in classpath $jars to $path")
    spark.stop()
  }
}
