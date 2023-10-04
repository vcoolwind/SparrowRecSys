package com.sparrowrecsys.offline.spark.model

import com.sparrowrecsys.offline.spark.SparkUtil
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.net.URL
import scala.collection.mutable

/**
 * 协同过滤模型
 */
object CollaborativeFiltering {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.spark("collaborativeFiltering")
    val ratingResourcesPath = this.getClass.getResource("/webroot/sampledata/ratings.csv")
    val path = this.getClass.getResource("/").getPath
    val index = path.indexOf("SparrowRecSys")
    val modelDir = path.substring(0, index + "SparrowRecSys".length) + "/output_model/collaborative_model"

    println(modelDir)
    loadModel(modelDir, spark)
  }

  def loadModel(modelDir: String, spark: SparkSession): Unit = {
    val alsModel = ALSModel.load(modelDir)
    //在Spark中，import spark.implicits._是一个用于导入隐式转换的语句，通常需要在创建SparkSession对象之后使用。
    //    getRecommend(alsModel, 12, spark)
    //    getRecommend(alsModel, 16, spark)
    //    getRecommend(alsModel, 28, spark)

    val allUserRecommends = alsModel.recommendForAllUsers(20)
    val userRecommendMap = mutable.Map[Int, Seq[Int]]()

    allUserRecommends.collect().foreach(row => {
      val userId = row.getInt(0)
      val movieIds = row.getAs[Seq[Row]](1).map(row => row.getInt(0))
      userRecommendMap += (userId -> movieIds)
    })

    getRecommend(userRecommendMap, 8018)
    getRecommend(userRecommendMap, 13610)
    getRecommend(userRecommendMap, 12955)


    //    getRecommend(allUserRecommends, 12)
    //    getRecommend(allUserRecommends, 16)
    //    getRecommend(allUserRecommends, 28)
  }

  def getRecommend(userRecommendMap: mutable.Map[Int, Seq[Int]], userId: Int): Unit = {
    val start = System.currentTimeMillis()
    val seq = userRecommendMap.getOrElse(userId, Seq.empty)
    println("elapsed time: " + (System.currentTimeMillis() - start))
    print(userId + "->")
    println(seq)
  }

  def getRecommend(allUserRecommends: DataFrame, userId: Int): Unit = {
    val start = System.currentTimeMillis()
    val userRecommendations = allUserRecommends
      .filter(col("userIdInt") === userId)
      .select(col("recommendations"))
      .head()
      .getAs[Seq[Row]](0)
    println("elapsed time: " + (System.currentTimeMillis() - start))
    print(userId + "->")
    println(userRecommendations.map(row => row.getInt(0)))
  }

  def getRecommend(alsModel: ALSModel, userId: Int, spark: SparkSession): Unit = {
    import spark.implicits._
    val users = Seq(userId).toDF("userIdInt").as[Int]
    val start = System.currentTimeMillis()
    val recommendItems1 = alsModel.recommendForUserSubset(users, 10)
    println("elapsed time: " + (System.currentTimeMillis() - start))
    recommendItems1.foreach(row => {
      print(row.get(0) + ">>")
      println(row.get(1))
    })
  }


  def generateModel(samplesDir: URL, modelDir: String, spark: SparkSession): Unit = {
    val toInt = udf[Int, String](_.toInt)
    val toFloat = udf[Double, String](_.toFloat)
    val ratingSamples = spark.read.format("csv").option("header", "true").load(samplesDir.getPath)
      .withColumn("userIdInt", toInt(col("userId")))
      .withColumn("movieIdInt", toInt(col("movieId")))
      .withColumn("ratingFloat", toFloat(col("rating")))

    // 调用randomSplit方法将ratingSamples数据集随机分成两个子数据集。
    // 该方法接受一个数组作为参数，该数组包含两个元素，表示每个子数据集的比例。
    // 在上述代码中，我们使用Array(0.8, 0.2)作为参数，表示将数据集分成80%的训练数据和20%的测试数据。
    // trainingData 用户训练的数据集；testingData 用户测试的数据集
    val Array(trainingData, testData) = ratingSamples.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    // ALS（Alternating Least Squares）模型是一种协同过滤推荐算法，用于预测用户对物品的评分。
    // 该模型基于矩阵分解技术，将用户评分矩阵分解成两个低维矩阵，分别表示用户和物品的特征向量。
    // 通过这些特征向量，可以计算用户对物品的预测评分。
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userIdInt")
      .setItemCol("movieIdInt")
      .setRatingCol("ratingFloat")

    // 训练模型
    val model = als.fit(trainingData)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(testData)

    // 模型的商品向量
    model.itemFactors.show(10, truncate = false)
    // 模型的用户向量
    model.userFactors.show(10, truncate = false)

    // 创建一个新的回归评估器对象evaluator。该评估器用于计算预测结果与实际结果之间的误差，并生成相应的评估指标。
    // 均方根误差（RMSE）是一种常用的回归模型评估指标，用于评估模型的预测精度。RMSE值越小，表示模型的预测精度越高。
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse") // 评估指标的名称,设置为rmse，rmse 表示均方根误差
      .setLabelCol("ratingFloat") //设置实际结果的列名,表示评分的浮点数值
      .setPredictionCol("prediction") //设置预测结果的列名
    // 使用evaluate方法计算预测结果的均方根误差。
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    //    // 为每个用户生成10个物品推荐策略
    //    val userRecs = model.recommendForAllUsers(10)
    //    userRecs.count()
    //    userRecs.show(false)
    //    // 为每个物品生成10个推荐用户
    //    val movieRecs = model.recommendForAllItems(10)
    //    movieRecs.count()
    //    movieRecs.show(false)


    //    // 用例集选择三个用户，并为其生成10个推荐物品
    //    val users = ratingSamples.select(als.getUserCol).distinct().limit(3)
    //    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    //    userSubsetRecs.show(false)
    //    // 用例集选择三个物品，并为其生成10个推荐人
    //    val movies = ratingSamples.select(als.getItemCol).distinct().limit(3)
    //    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    //    movieSubSetRecs.show(false)
    //val users = ratingSamples.select(als.getUserCol).distinct().limit(3)
    import spark.implicits._
    val users = Seq(28).toDF("userIdInt").as[Int]
    val recommendItems1 = model.recommendForUserSubset(users, 10)
    recommendItems1.foreach(row => {
      println(row.get(0))
      println(row.get(1))
    })
    //设置ALS模型的超参数
    val paramGrid = new ParamGridBuilder()
      //      .addGrid(als.regParam, Array(0.05, 0.1))
      //      .addGrid(als.rank, Array(10, 20))
      .addGrid(als.regParam, Array(0.1))
      .addGrid(als.rank, Array(10))
      .build()

    //    CrossValidator是Spark MLlib中的一个交叉验证工具，用于评估和选择机器学习模型的性能。
    //    它可以帮助我们选择最优的超参数和模型配置，以提高模型的预测精度和泛化能力。
    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)
    val cvModel = cv.fit(testData)
    val bestModel = cvModel.bestModel.asInstanceOf[ALSModel]

    val recommendItems2 = bestModel.recommendForUserSubset(users, 10)
    recommendItems2.foreach(row => {
      println(row.get(0))
      println(row.get(1))
    })

    bestModel.save(modelDir)
    spark.stop()
  }
}