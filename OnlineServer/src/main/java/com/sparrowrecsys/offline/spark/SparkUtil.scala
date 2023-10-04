package com.sparrowrecsys.offline.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkUtil {
  def spark(appName:String): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.submit.deployMode", "client")
      .config("spark.version", "2.4.0")
      .getOrCreate()
    return spark
  }

  def toDF(spark: SparkSession,rdd: RDD[Row], elems: StructField*): DataFrame = {
    val schema = StructType(elems.toSeq)
    return spark.createDataFrame(rdd, schema)
  }

}
