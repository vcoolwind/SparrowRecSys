package com.stone.study.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ToDFExample {
  def main(args: Array[String]): Unit = {
    test1()
  }

  def test1(): Unit = {
    // 创建 Seq
    val data = Seq(
      ("Alice", "math", 80),
      ("Bob", "math", 75),
      ("Charlie", "math", 90),
      ("Alice", "english", 90),
      ("Bob", "english", 88),
      ("Charlie", "english", 79)
    )
    val subjectScores = SparkUtil.spark()
      .createDataFrame(data)
      .toDF("name", "subject", "score")
    subjectScores.show()

    subjectScores.filter("score > 80").show()
    subjectScores.where("score > 80").show()

    subjectScores.groupBy("name").sum("score").show()
    subjectScores.groupBy("subject").max("score").show()
  }

  def test2(): Unit = {
    // 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .appName("ToDFExample")
      .master("local[*]")
      .getOrCreate()
    // 创建 Seq
    val data = Seq(
      ("Alice", "math", 80),
      ("Bob", "math", 75),
      ("Charlie", "math", 90),
      ("Alice", "english", 90),
      ("Bob", "english", 88),
      ("Charlie", "english", 79)
    )
    // 创建 DataFrame 的结构信息
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("subject", StringType, nullable = false),
      StructField("score", IntegerType, nullable = false)
    ))

    // 将 Seq 转换为 RDD[Row]
    val rdd = spark.sparkContext.parallelize(data).map(row => Row(row._1, row._2, row._3))

    // 使用 createDataFrame 函数将 RDD[Row] 转换为 DataFrame
    val df = spark.createDataFrame(rdd, schema)

    // 输出 DataFrame
    df.show()
    df.filter("score > 80").show()

    df.groupBy("name").sum("score").show()
    df.groupBy("subject").max("score").show()

    spark.close()
  }
}