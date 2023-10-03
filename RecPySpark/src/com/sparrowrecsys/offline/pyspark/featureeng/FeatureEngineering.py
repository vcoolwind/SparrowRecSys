from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, QuantileDiscretizer, MinMaxScaler
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

from com.sparrowrecsys.offline.pyspark.fileutil import getResourcesDir


def oneHotEncoderExample(movieSamples):
  print("----------oneHotEncoderExample----------")
  # csv的movieId 默认是string，这里映射列名为movieIdNumber，类型为Integer，多一列映射
  samplesWithIdNumber = movieSamples.withColumn("movieIdNumber", F.col("movieId").cast(IntegerType()))
  samplesWithIdNumber.printSchema()
  samplesWithIdNumber.show(10)

  #  OneHotEncoder中，输入列为movieIdNumber，输出列为movieIdVector
  # //利用Spark的机器学习库Spark MLlib创建One-hot编码器
  encoder =  OneHotEncoder(
    inputCols=["movieIdNumber"],
    outputCols=['movieIdVector'],
    dropLast=False
  )

  # 训练One-hot编码器，并完成从id特征到One-hot向量的转换
  oneHotEncoderSamples = encoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber)
  # 打印最终样本的数据结构
  oneHotEncoderSamples.printSchema()
  # 打印10条样本查看结果
  oneHotEncoderSamples.show(10)

  # movieIdVector中的1000是总维度么，为什么是1000，movieId最大是999 ？
  # +-------+--------------------+--------------------+-------------+-----------------+
  # |movieId|               title|              genres|movieIdNumber|    movieIdVector|
  # +-------+--------------------+--------------------+-------------+-----------------+
  # |      1|    Toy Story (1995)|Adventure|Animati...|            1| (1000,[1],[1.0])|
  # |      2|      Jumanji (1995)|Adventure|Childre...|            2| (1000,[2],[1.0])|
  # |      3|Grumpier Old Men ...|      Comedy|Romance|            3| (1000,[3],[1.0])|

  # 在 Spark 中，OneHotEncoder 的输出结果是一个 Vector 数据结构，表示经过独热编码后的稀疏向量。
  # Vector 是 Spark 中表示向量的数据结构，支持稠密向量和稀疏向量两种类型。
  #
  # 对于 OneHotEncoder 输出的稀疏向量，其数据结构包含三个部分：
  #     size：表示向量的长度，即独热编码后的特征数量。
  #     indices：表示非零元素所在的下标位置，是一个整型数组。
  #     values：表示非零元素的值，是一个双精度浮点数数组。

def array2vec(genreIndexes, indexSize):
  genreIndexes.sort()
  fill_list = [1.0 for _ in range(len(genreIndexes))]
  return Vectors.sparse(indexSize, genreIndexes, fill_list)


def multiHotEncoderExample(movieSamples):
  samplesWithGenre = movieSamples.select("movieId", "title", explode(
    split(F.col("genres"), "\\|").cast(ArrayType(StringType()))).alias('genre'))
  genreIndexer = StringIndexer(inputCol="genre", outputCol="genreIndex")
  StringIndexerModel = genreIndexer.fit(samplesWithGenre)
  genreIndexSamples = StringIndexerModel.transform(samplesWithGenre).withColumn(
    "genreIndexInt",
    F.col("genreIndex").cast(IntegerType()))
  indexSize = genreIndexSamples.agg(max(F.col("genreIndexInt"))).head()[0] + 1
  processedSamples = genreIndexSamples.groupBy('movieId').agg(
    F.collect_list('genreIndexInt').alias('genreIndexes')).withColumn(
    "indexSize", F.lit(indexSize))
  finalSample = processedSamples.withColumn("vector",
                                            udf(array2vec, VectorUDT())(
                                              F.col("genreIndexes"),
                                              F.col("indexSize")))
  finalSample.printSchema()
  finalSample.show(10)


def ratingFeatures(ratingSamples):
  ratingSamples.printSchema()
  ratingSamples.show()
  # calculate average movie rating score and rating count
  movieFeatures = ratingSamples.groupBy('movieId').agg(
    F.count(F.lit(1)).alias('ratingCount'),
    F.avg("rating").alias("avgRating"),
    F.variance('rating').alias('ratingVar')) \
    .withColumn('avgRatingVec',
                udf(lambda x: Vectors.dense(x), VectorUDT())('avgRating'))
  movieFeatures.show(10)
  # bucketing
  ratingCountDiscretizer = QuantileDiscretizer(numBuckets=100,
                                               inputCol="ratingCount",
                                               outputCol="ratingCountBucket")
  # Normalization
  ratingScaler = MinMaxScaler(inputCol="avgRatingVec",
                              outputCol="scaleAvgRating")
  pipelineStage = [ratingCountDiscretizer, ratingScaler]
  featurePipeline = Pipeline(stages=pipelineStage)
  movieProcessedFeatures = featurePipeline.fit(movieFeatures).transform(
    movieFeatures)
  movieProcessedFeatures.show(10)


if __name__ == '__main__':
  # 注册一个spark实例
  conf = SparkConf().setAppName('featureEngineering').setMaster('local')
  spark = SparkSession.builder.config(conf=conf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  file_path = getResourcesDir()

  # 加载数据文件，样本集中的每一条数据代表一部电影的信息 movies.csv的数据格式：movieId,title,genres
  movieResourcesPath = file_path + "/webroot/sampledata/movies.csv"
  # csv有header，读取头部
  movieSamples = spark.read.format('csv').option('header', 'true').load(movieResourcesPath)
  print("Raw Movie Samples:")
  movieSamples.printSchema()
  # 显示10行读取的数据
  movieSamples.show(10)

  # 读取打分信息 userId（打分人）,movieId（电影id）,rating（打分数值）,timestamp（打分时间）
  print("Numerical features Example:")
  ratingsResourcesPath = file_path + "/webroot/sampledata/ratings.csv"
  ratingSamples = spark.read.format('csv').option('header', 'true').load(ratingsResourcesPath)
  ratingSamples.printSchema()
  ratingSamples.show(10)

  oneHot = True
  multiHot = False
  ratingFeatures = False

  if oneHot:
    # one hot 编码
    print("OneHotEncoder Example:")
    oneHotEncoderExample(movieSamples)

  if multiHot:
    # multi hot 编码
    print("MultiHotEncoder Example:")
    multiHotEncoderExample(movieSamples)

  if ratingFeatures:
    # 计算打分特征？
    ratingFeatures(ratingSamples)
