package com.stone.study.spark

import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 在 Spark 中，OneHotEncoder 是一种常用的特征编码方法，用于将分类变量转换为数值型变量。
 * OneHotEncoder 将<每个分类变量>的取值转换为一个<二元向量>，<向量的长度等于分类变量的取值个数>，向量中只有一个元素为 1，其余元素为 0。
 * 例如，对于一个有三个取值的分类变量，OneHotEncoder 将其转换为一个长度为 3 的二元向量，向量中只有一个元素为 1，其余元素为 0。
 *
 * 在 Spark 中，使用 OneHotEncoder 需要先调用 fit 函数进行拟合，然后再调用 transform 函数进行转换。
 * fit 函数的作用是获取数据集中每个分类变量的取值，并构建一个编码器模型。transform 函数则使用编码器模型将分类变量转换为二元向量。
 */
object MovieIdVectorWithOneHotEncoder {

  def main(args: Array[String]): Unit = {
    // 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("OneHotEncoderExample")
      .getOrCreate()

    val data1 = Seq(
      (1, "Toy Story (1995)"),
      (2, "Jumanji (1995)"),
      (3, "Grumpier Old Men (1995)"),
      (4, "Waiting to Exhale (1995)")
    )

    val data2 = Seq(
      (1, "Toy Story (1995)"),
      (2, "Jumanji (1995)"),
      (3, "Grumpier Old Men (1995)"),
      (4, "Waiting to Exhale (1995)"),
      (1, "Toy Story (1995)"),
      (2, "Jumanji (1995)")
    )

    val movieSamples1 = getDataDF(spark, data1)
    val vector1 = oneHotEncoderExample(movieSamples1)
    val movieSamples2 = getDataDF(spark, data2)
    val vector2 = oneHotEncoderExample(movieSamples2)

    vector1.show(10)
    vector2.show(10)
    // 相同的id会被忽略
    println(vector1.equals(vector2))

    val vectors = vector2.select(col("movieIdVector")).collect()
    vectors.foreach(
      row => {
        println(row.toString())
        println(row.getAs[Int](0))
      }
    )

  }

  def oneHotEncoderExample(samples: DataFrame): DataFrame = {
    // samples的movieId映射为movieIdNumber，并制定类型为int。 相当于数据集复制一个数字的字段？
    val samplesWithIdNumber = samples.withColumn("movieIdNumber", col("movieId").cast(sql.types.IntegerType))
    samplesWithIdNumber.printSchema()
    samplesWithIdNumber.show(10)

    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array("movieIdNumber")) // 指明对movieIdNumber进行向量化,数值型的分类变量
      .setOutputCols(Array("movieIdVector")) //设置输出列名为 "movieIdVector"。编码后的结果会存储在这个列中。
      .setDropLast(false)
    // setDropLast 设置是否删除最后一个值。
    // 如果设置为 true，则会删除最后一个值，即输出向量中只有 n-1 个元素，其中 n 是分类变量的取值个数。
    // 如果设置为 false，则会保留所有值，即输出向量中有 n 个元素。这里设置为 false，表示保留所有值。

    val oneHotEncoderSamples = oneHotEncoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber)
    oneHotEncoderSamples.printSchema()
    oneHotEncoderSamples.show(10)
    return oneHotEncoderSamples
  }

  def getDataDF(spark: SparkSession, data: Seq[(Int, String)]): DataFrame = {
    // 创建 DataFrame 的结构信息
    val schema = StructType(Seq(
      StructField("movieId", IntegerType, nullable = false),
      StructField("title", StringType, nullable = false),
    ))

    // 将 Seq 转换为 RDD[Row]
    val rdd = spark.sparkContext.parallelize(data).map(row => Row(row._1, row._2))
    // 使用 createDataFrame 函数将 RDD[Row] 转换为 DataFrame
    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show(10)
    return df
  }

}

