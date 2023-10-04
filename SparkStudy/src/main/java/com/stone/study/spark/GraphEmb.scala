package com.stone.study.spark

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object GraphEmb {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtil.spark()
    // 获取用户对物品的行为序列图
    val seqData = processItemSequence(sparkSession)
    // 构建物品关系图
    generateTransitionMatrix(seqData)
  }

  def processItemSequence(sparkSession: SparkSession): RDD[Seq[String]] = {
    //    读取 ratings 原始数据到 Spark 平台；
    //    用 where 语句过滤评分低的评分记录；
    //    用 groupBy userId 操作聚合每个用户的评分记录，DataFrame 中每条记录是一个用户的评分序列；
    //    定义一个自定义操作 sortUdf，用它实现每个用户的评分记录按照时间戳进行排序；
    //    把每个用户的评分记录处理成一个字符串的形式，供后续训练过程使用。
    val ratingsResourcesPath = this.getClass.getResource("/sampledata/ratings_simple.csv")
    val ratingSamples = sparkSession.read
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
      .where(col("rating") >= 2.5) //过滤掉评分在3.5一下的评分记录
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

  //samples 输入的观影序列样本集，从物品 A 到物品 B 的跳转概率
  def generateTransitionMatrix(samples: RDD[Seq[String]]): (mutable.Map[String, mutable.Map[String, Double]], mutable.Map[String, Double]) = {
    println(samples.count())
    //通过flatMap操作把观影序列打碎成一个个影片对，相邻的数组组成一对：如457 597 919，会组成2对（457:597,597:919）
    val pairSamples = samples.flatMap[(String, String)](sample => {
      var pairSeq = Seq[(String, String)]()
      var previousItem: String = null
      sample.foreach((element: String) => {
        if (previousItem != null) {
          pairSeq = pairSeq :+ (previousItem, element)
        }
        previousItem = element
      })
      pairSeq
    })
    println("电影对数明细")
    pairSamples.foreach(pair => println(pair))

    //统计影片对的数量，样例数据：(597:919,3)
    val pairCountMap = pairSamples.countByValue()
    println("电影对数汇总")
    pairCountMap.foreach(pairCount => println(pairCount))

    var pairTotalCount = 0L
    //转移概率矩阵的双层Map数据结构 key 商品id，value是map（key是商品关联对的key，value是出现次数）
    val transitionCountMatrix = mutable.Map[String, mutable.Map[String, Long]]()
    // 商品对的前一个商品的出现次数
    val itemCountMap = mutable.Map[String, Long]()

    pairCountMap.foreach(pair => {
      // 商品对
      val pairItems = pair._1
      // 商品对出现的次数
      val count = pair._2
      // 商品对的前一个
      val item_pre = pairItems._1
      // 商品对的后一个
      val item_last = pairItems._2

      if (!transitionCountMatrix.contains(item_pre)) {
        transitionCountMatrix(item_pre) = mutable.Map[String, Long]()
      }

      transitionCountMatrix(item_pre)(item_last) = count
      itemCountMap(item_pre) = itemCountMap.getOrElse[Long](item_pre, 0) + count
      // 商品对总数累计
      pairTotalCount = pairTotalCount + count
    })

    println("---- transitionCountMatrix -----")
    transitionCountMatrix.foreach(map => {
      println(map)
    })

    println("---- itemCountMap -----")
    itemCountMap.foreach(map => {
      println(map)
    })


    // 计算马尔可夫链的转移矩阵和影片的分布向量
    // 转移矩阵分布向量，把无限转为 [0,1]的映射
    val transitionMatrix = mutable.Map[String, mutable.Map[String, Double]]()
    // 物品分布向量
    val itemDistribution = mutable.Map[String, Double]()

    transitionCountMatrix.foreach {
      case (itemAId, transitionMap) =>
        transitionMatrix(itemAId) = mutable.Map[String, Double]()
        transitionMap.foreach {
          // 商品映射对的每个数值和映射总数的比值
          case (itemBId, transitionCount) =>
            transitionMatrix(itemAId)(itemBId) = transitionCount.toDouble / itemCountMap(itemAId)
        }
    }
    itemCountMap.foreach {
      // 每个商品id和总数的映射向量
      case (itemId, itemCount) => itemDistribution(itemId) = itemCount.toDouble / pairTotalCount
    }
    transitionMatrix.foreach(entry=>println(entry))
    itemDistribution.foreach(entry=>println(entry))

    return (transitionMatrix, itemDistribution)

  }

  //  //随机游走采样函数
  //  //transferMatrix 转移概率矩阵
  //  //itemCount 物品出现次数的分布
  //  def randomWalk(transferMatrix: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount: scala.collection.mutable.Map[String, Long]): Seq[Seq[String]] = {
  //    //样本的数量
  //    val sampleCount = 20000
  //    //每个样本的长度
  //    val sampleLength = 10
  //    val samples = scala.collection.mutable.ListBuffer[Seq[String]]()
  //
  //    //物品出现的总次数
  //    var itemTotalCount: Long = 0
  //    for ((k, v) <- itemCount) itemTotalCount += v
  //
  //
  //    //随机游走sampleCount次，生成sampleCount个序列样本
  //    for (w <- 1 to sampleCount) {
  //      samples.append(oneRandomWalk(transferMatrix, itemCount, itemTotalCount, sampleLength))
  //    }
  //
  //
  //    Seq(samples.toList: _*)
  //  }

  //  def randomWalk(transitionMatrix: mutable.Map[String, mutable.Map[String, Double]], itemDistribution: mutable.Map[String, Double], sampleCount: Int, sampleLength: Int): Seq[Seq[String]] = {
  //    val samples = mutable.ListBuffer[Seq[String]]()
  //    for (_ <- 1 to sampleCount) {
  //      samples.append(oneRandomWalk(transitionMatrix, itemDistribution, sampleLength))
  //    }
  //    Seq(samples.toList: _*)
  //  }

  def graphEmb(samples: RDD[Seq[String]], sparkSession: SparkSession, embLength: Int, embOutputFilename: String, saveToRedis: Boolean, redisKeyPrefix: String): Word2VecModel = {
    val transitionMatrixAndItemDis = generateTransitionMatrix(samples)

    println(transitionMatrixAndItemDis._1.size)
    println(transitionMatrixAndItemDis._2.size)

    val sampleCount = 20000
    val sampleLength = 10
    val newSamples = randomWalk(transitionMatrixAndItemDis._1, transitionMatrixAndItemDis._2, sampleCount, sampleLength)

    val rddSamples = sparkSession.sparkContext.parallelize(newSamples)
    trainItem2vec(sparkSession, rddSamples, embLength, embOutputFilename, saveToRedis, redisKeyPrefix)
  }

  def trainItem2vec(sparkSession: SparkSession, samples: RDD[Seq[String]], embLength: Int, embOutputFilename: String, saveToRedis: Boolean, redisKeyPrefix: String): Word2VecModel = {
    val word2vec = new Word2Vec()
      .setVectorSize(embLength)
      .setWindowSize(5)
      .setNumIterations(10)

    val model = word2vec.fit(samples)


    val synonyms = model.findSynonyms("158", 20)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    val embFolderPath = this.getClass.getResource("/webroot/modeldata/")
    val file = new File(embFolderPath.getPath + embOutputFilename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (movieId <- model.getVectors.keys) {
      bw.write(movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
    }
    bw.close()

    embeddingLSH(sparkSession, model.getVectors)
    model
  }

  def embeddingLSH(spark: SparkSession, movieEmbMap: Map[String, Array[Float]]): Unit = {

    val movieEmbSeq = movieEmbMap.toSeq.map(item => (item._1, Vectors.dense(item._2.map(f => f.toDouble))))
    val movieEmbDF = spark.createDataFrame(movieEmbSeq).toDF("movieId", "emb")

    //LSH bucket model
    val bucketProjectionLSH = new BucketedRandomProjectionLSH()
      .setBucketLength(0.1)
      .setNumHashTables(3)
      .setInputCol("emb")
      .setOutputCol("bucketId")

    val bucketModel = bucketProjectionLSH.fit(movieEmbDF)
    val embBucketResult = bucketModel.transform(movieEmbDF)
    println("movieId, emb, bucketId schema:")
    embBucketResult.printSchema()
    println("movieId, emb, bucketId data result:")
    embBucketResult.show(10, truncate = false)

    println("Approximately searching for 5 nearest neighbors of the sample embedding:")
    val sampleEmb = Vectors.dense(0.795, 0.583, 1.120, 0.850, 0.174, -0.839, -0.0633, 0.249, 0.673, -0.237)
    bucketModel.approxNearestNeighbors(movieEmbDF, sampleEmb, 5).show(truncate = false)
  }


  def randomWalk(transitionMatrix: mutable.Map[String, mutable.Map[String, Double]],
                 itemDistribution: mutable.Map[String, Double],
                 sampleCount: Int,
                 sampleLength: Int): Seq[Seq[String]] = {
    val samples = mutable.ListBuffer[Seq[String]]()
    for (_ <- 1 to sampleCount) {
      samples.append(oneRandomWalk(transitionMatrix, itemDistribution, sampleLength))
    }
    Seq(samples.toList: _*)
  }

  /**
   * 通过随机游走产生一个样本的过程
   *
   * @param transitionMatrix 转移概率矩阵
   * @param itemDistribution 物品出现总次数
   * @param sampleLength     每个样本的长度
   * @return
   */
  def oneRandomWalk(transitionMatrix: mutable.Map[String, mutable.Map[String, Double]],
                    itemDistribution: mutable.Map[String, Double],
                    sampleLength: Int): Seq[String] = {
    val sample = mutable.ListBuffer[String]()

    //pick the first element
    val randomDouble = Random.nextDouble()
    var firstItem = ""
    var accumulateProb: Double = 0D
    breakable {
      for ((item, prob) <- itemDistribution) {
        accumulateProb += prob
        if (accumulateProb >= randomDouble) {
          firstItem = item
          break
        }
      }
    }

    sample.append(firstItem)
    var curElement = firstItem

    breakable {
      for (_ <- 1 until sampleLength) {
        if (!itemDistribution.contains(curElement) || !transitionMatrix.contains(curElement)) {
          break
        }

        val probDistribution = transitionMatrix(curElement)
        val randomDouble = Random.nextDouble()
        var accumulateProb: Double = 0D
        breakable {
          for ((item, prob) <- probDistribution) {
            accumulateProb += prob
            if (accumulateProb >= randomDouble) {
              curElement = item
              break
            }
          }
        }
        sample.append(curElement)
      }
    }
    Seq(sample.toList: _*)
  }

}
