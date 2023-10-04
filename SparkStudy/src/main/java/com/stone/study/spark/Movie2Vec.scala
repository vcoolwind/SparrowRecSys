package com.stone.study.spark

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

object Movie2Vec {
  def main(args: Array[String]): Unit = {
    val movieModelDir = this.getClass.getResource("/").getPath + "train_data/movie_embedding"

    //处理原始数据
    val movieSeq = processItemSequence()

    //训练模型，并保存在指定模型目录中
    trainItem2vec(movieModelDir, movieSeq)

    // 加载模型
    val loadedModel = Word2VecModel.load(movieModelDir)

    // 查找与单词"592"最相似的20个单词（不需要自己计算了）
    val synonyms = loadedModel.findSynonyms("592", 20)
    synonyms.show()
  }

  def processItemSequence(): DataFrame = {
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
    //把所有id连接成一个String，方便后续word2vec模型处理
    userSeq.show(20)
    return userSeq
  }

  def trainItem2vec(embeddingDir: String, samples: DataFrame): Unit = {
    //设置模型参数
    val word2vec = new Word2Vec()
      .setVectorSize(10)
      .setWindowSize(5)
      .setNumPartitions(10)
      .setInputCol("movieIds")
      .setOutputCol("embedding")

    //训练模型
    val model = word2vec.fit(samples)

    //训练结束，用模型查找与item"592"最相似的20个item
    val synonyms = model.findSynonyms("592", 20)
    synonyms.show()

    //保存模型
    model.save(embeddingDir)
  }
}
