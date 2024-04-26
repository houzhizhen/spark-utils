package com.baidu.spark.utils.shuffle

import com.baidu.spark.util.RandomUtil
import org.apache.spark.sql.SparkSession

object ESSTest {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: ESSTest <mapPartitions> <outputCountPerMapPartition> <reducePartitions>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("Spark ESSTest")
      .getOrCreate()

    val mapPartitions = args(0).toInt
    val outputCountPerMapPartition = args(1).toLong
    val reducePartitions = args(2).toInt  // 每个reduce task的分区数
    println("mapPartitions=" + mapPartitions)
    println("outputCountPerMapPartition=" + outputCountPerMapPartition)
    println("reducePartitions=" + reducePartitions)

    val sc = spark.sparkContext
    val data = 0 to mapPartitions
    val distData = sc.parallelize(data, mapPartitions)
    val mapRDD = distData.mapPartitionsWithIndex((i, iter) =>
      LongPairIterator(i, outputCountPerMapPartition))
    val reduceRDD = mapRDD.repartition(reducePartitions)
    val sum = reduceRDD.mapPartitionsWithIndex((i, iter) =>  {
      var sum = 0L
      while(iter.hasNext){
        sum += iter.next()._2.length
      }
      println("reduce task " + i + " sum=" + sum)
      Iterator(sum)
    }).reduce((a, b) => a + b)
    println("sum=" + sum)

    spark.stop()
  }
}
case class LongPairIterator(index: Int, outputCountPerMapPartition: Long)
  extends Iterator[(Long, Array[Byte])] {
  val bytes = RandomUtil.randomByteArray(1024)

  var value = 0L
  override def hasNext: Boolean = value < outputCountPerMapPartition
  override def next(): (Long, Array[Byte]) = {
    val r = value
    value += 1
    RandomUtil.randomByteArray(bytes, 10)
    (r, bytes)
  }
}