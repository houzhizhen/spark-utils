package com.baidu.spark.sql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object TestSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestSql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    val schemaString = "c1"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = distData.map(i => Row(i))
    val idDF = spark.createDataFrame(rowRDD, schema)
    idDF.createOrReplaceTempView("t1")
    spark.sql("select count(c1) from t1").show()
    idDF.show()
    idDF.printSchema()

    spark.stop()
  }
}
