package com.stone.study.spark

import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("OneHotEncoderExample")
      .getOrCreate()

    // 创建 DataFrame
    val data = Seq(
      (0, "male"),
      (1, "female"),
      (2, "unknown"),
      (3, "male")
    )

    // 创建 DataFrame 的结构信息
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("gender", StringType, nullable = false),
    ))

    // 将 Seq 转换为 RDD[Row]
    val rdd = spark.sparkContext.parallelize(data).map(row => Row(row._1, row._2))
    // 使用 createDataFrame 函数将 RDD[Row] 转换为 DataFrame
    val df = spark.createDataFrame(rdd, schema)

    // 使用 StringIndexer 进行标签编码
    val indexer = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("genderIndex")
      .fit(df)

    val indexed = indexer.transform(df)

    // 使用 OneHotEncoder 进行独热编码
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("genderIndex"))
      .setOutputCols(Array("genderVec"))

    val encoded = encoder.fit(indexed).transform(indexed)

    // 输出独热编码结果
    encoded.select("gender", "genderVec").show()
    encoded.show(10)
    // 查看独热编码结果的数据结构
    val vec = encoded.select("genderVec").head().getAs[Vector](0)
    vec.toArray.foreach(f=> println(f))
    //    println(vec.size)
    //    println(vec.indices.mkString(","))
    //    println(vec.values.mkString(","))
  }
}