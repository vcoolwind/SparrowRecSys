package com.stone.study.spark


import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.{BufferedWriter, File, FileWriter}

object Movie2VecWithMllib {
  def main(args: Array[String]): Unit = {
    val movieSeq = processItemSequence()
    trainItem2vec(movieSeq)
  }

  def processItemSequence(): RDD[Seq[String]] = {
    //    读取 ratings 原始数据到 Spark 平台；
    //    用 where 语句过滤评分低的评分记录；
    //    用 groupBy userId 操作聚合每个用户的评分记录，DataFrame 中每条记录是一个用户的评分序列；
    //    定义一个自定义操作 sortUdf，用它实现每个用户的评分记录按照时间戳进行排序；
    //    把每个用户的评分记录处理成一个字符串的形式，供后续训练过程使用。
    val ratingsResourcesPath = this.getClass.getResource("/sampledata/ratings.csv")
    val ratingSamples = SparkUtil.spark().read
      .format("csv")
      .option("header", "true")
      .load(ratingsResourcesPath.getPath)
    ratingSamples.show(20)

    //实现一个用户定义的操作函数(UDF)，用于之后的排序
    val sortUdf: UserDefinedFunction = udf((rows: Seq[Row]) => {
      rows.map { case Row(movieId: String, timestamp: String) => (movieId, timestamp) }
        .sortBy { case (movieId, timestamp) => timestamp }
        .map { case (movieId, timestamp) => movieId }
    })


    //把原始的rating数据处理成序列数据
    val userSeq = ratingSamples
      .where(col("rating") >= 3.5) //过滤掉评分在3.5一下的评分记录
      .groupBy("userId") //按照用户id分组
      .agg(sortUdf(collect_list(struct("movieId", "timestamp"))) as "movieIds") //每个用户生成一个序列并用刚才定义好的udf函数按照timestamp排序
      .withColumn("movieIdStr", array_join(col("movieIds"), " "))
    //把所有id连接成一个String，方便后续word2vec模型处理
    userSeq.show(20)

    //把序列数据筛选出来，丢掉其他过程数据
    val goalSeq = userSeq
      .select("movieIdStr")
      .rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)

    return goalSeq
  }

  def trainItem2vec(samples: RDD[Seq[String]]): Unit = {
    //设置模型参数
    val word2vec = new Word2Vec()
      .setVectorSize(10)
      .setWindowSize(5)
      .setNumIterations(10)


    //训练模型
    val model = word2vec.fit(samples)

    //训练结束，用模型查找与item"592"最相似的20个item
    val synonyms = model.findSynonyms("592", 20)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    //保存模型
    val embFolderPath = this.getClass.getResource("/")
    val rootDir = embFolderPath.getPath + "traindata/"
    new File(rootDir).mkdirs()
    val file = new File(rootDir + "embedding.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    var id = 0
    //用model.getVectors获取所有Embedding向量
    for (movieId <- model.getVectors.keys) {
      id += 1
      bw.write(movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
    }
    bw.close()
  }

  def processItemSequenceV2(): Unit = {
    //    读取 ratings 原始数据到 Spark 平台；
    //    用 where 语句过滤评分低的评分记录；
    //    用 groupBy userId 操作聚合每个用户的评分记录，DataFrame 中每条记录是一个用户的评分序列；
    //    定义一个自定义操作 sortUdf，用它实现每个用户的评分记录按照时间戳进行排序；
    //    把每个用户的评分记录处理成一个字符串的形式，供后续训练过程使用。
    val ratingsResourcesPath = this.getClass.getResource("/sampledata/ratings.csv")
    val ratingSamples = SparkUtil.spark().read
      .format("csv")
      .option("header", "true")
      .schema(StructType(Seq(
        StructField("userId", IntegerType, nullable = false),
        StructField("movieId", IntegerType, nullable = false),
        StructField("rating", DoubleType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
      )))
      .load(ratingsResourcesPath.getPath)
    ratingSamples.show(20)


    val goalSamples = ratingSamples
      .filter(row => row.getDouble(2) > 3.5)
      .groupBy(col("userId"))
      .agg(collect_list("movieId") as "movieIds")
      .withColumn("movieIdStr", array_join(col("movieIds"), " "))
      .select("movieIdStr")
      .rdd
      .map(row => row.getAs[String]("movieIdStr").split(" ").toSeq)

    goalSamples.foreach(row => println(row))
  }
}
