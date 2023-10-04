package com.stone.study.spark

import com.stone.study.spark.SparkUtil._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}

import scala.collection.mutable.ArrayBuffer

object DatasetStudy {
  def main(args: Array[String]): Unit = {
    agg()
    flatMap()
  }

  def agg(): Unit = {
    val spark = SparkUtil.spark()
    val rdd = spark.sparkContext.parallelize(Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 35)
    )).map(row => Row(row._1, row._2))

    val df = toDF(spark, rdd,
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    )

    val result = df.agg(max("age"), min("age"), avg("age"))
    result.show()
  }

  def flatMap(): Unit = {
    val spark = SparkUtil.spark()
    val rdd = spark.sparkContext.parallelize(Seq(
      ("858 50 597 919"),
      ("356 597 919 986"),
      ("457 597 919")
    )).map(line => line.split(" ").toSeq)

    val pairSamples = rdd.flatMap[String](sample => {
      val pairSeq = new ArrayBuffer[String]()
      var previousItem: String = null
      sample.foreach((element: String) => {
        if (previousItem != null) {
          pairSeq += (previousItem + ":" + element)
        }
        previousItem = element
      })
      pairSeq
    })
    pairSamples.foreach(s=>println(s))
    val pairValues = pairSamples.countByValue()
    pairValues.foreach(s=>println(s))

  }
}
