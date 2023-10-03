package com.stone.study.spark

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
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
object MovieIdVectorWithMultiHotEncoder {

  def main(args: Array[String]): Unit = {
    // 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("OneHotEncoderExample")
      .getOrCreate()

    val data1 = Seq(
      (1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
      (2, "Jumanji (1995)", "Adventure|Children|Fantasy"),
      (3, "Grumpier Old Men (1995)", "Comedy|Romance"),
      (4, "Waiting to Exhale (1995)", "Comedy|Drama|Romance")
    )

    val movieSamples1 = getDataDF(spark, data1)
    multiHotEncoderExample(movieSamples1)
  }

  def multiHotEncoderExample(movieSamples: DataFrame): DataFrame = {
    // 选择需要的列
    //    val columns: Array[Column] = Array(
    //      col("movieId"),
    //      col("title"),
    //      explode(split(col("genres"), "\\|").cast("array<string>")).alias("genre")
    //    )
    //    val samplesWithGenre: DataFrame = movieSamples.select(columns: _*)

    // 选择需要的列,按列展开，形成多行
    val samplesWithGenre = movieSamples
      .select(
        col("movieId"),
        col("title"),
        explode(
          split(col("genres"), "\\|").cast("array<string>")
        ).alias("genre")
      )
    samplesWithGenre.show()

    //  对genre进行索引操作
    val genreIndexer = new StringIndexer()
      .setInputCol("genre")
      .setOutputCol("genreIndex")
    val genreIndexSamples = genreIndexer.fit(samplesWithGenre).transform(samplesWithGenre)
      .withColumn(("genreIndexInt"), col("genreIndex").cast(IntegerType))
    genreIndexSamples.show()

    // 计算 indexSize
    val indexSize = genreIndexSamples.agg(max(col("genreIndexInt"))).head().getInt(0) + 1

    // 对数据进行处理
    val processedSamples = genreIndexSamples.groupBy(col("movieId"))
      .agg(collect_list(col("genreIndexInt")).alias("genreIndexes"))
      .withColumn("indexSize", lit(indexSize))

    // 将 genreIndexes 转换为向量
    val array2vec = udf((array: Seq[Int], size: Int) => {
      Vectors.sparse(size, array.map(i => (i, 1.0)))
    })

    val finalSample = processedSamples.withColumn("vector", array2vec(col("genreIndexes"), col("indexSize")))
    finalSample.printSchema()
    finalSample.show(10, false)

    return null
  }

  def getDataDF(spark: SparkSession, data: Seq[(Int, String, String)]): DataFrame = {
    // 创建 DataFrame 的结构信息
    val schema = StructType(Seq(
      StructField("movieId", IntegerType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres", StringType, nullable = false),
    ))

    // 将 Seq 转换为 RDD[Row]
    val rdd = spark.sparkContext.parallelize(data).map(row => Row(row._1, row._2, row._3))
    // 使用 createDataFrame 函数将 RDD[Row] 转换为 DataFrame
    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show(10)
    return df
  }

}

