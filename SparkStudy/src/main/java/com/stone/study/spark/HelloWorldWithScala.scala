package com.stone.study.spark

import org.apache.spark.sql.{Encoders, SparkSession, functions}


object HelloWorldWithScala {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ScalaWordCount")
      .master("local[*]")
      .getOrCreate()


    val textContent = spark.read.textFile(FileUtils.getClassPathFile("helloworld.md"))
    //    println(textContent)
    //    println(textContent.count())
    //    println(textContent.first())
    //    textContent.foreach(world => println(world))

    // 统计每个单词的个数
    val words = textContent.flatMap(line => line.split(" "))(Encoders.STRING)
      .toDF("word")
    val counts = words.groupBy(functions.col("word"))
      .count()
      .sort(functions.col("count").desc)


    // 输出计数结果
    counts.show()

    //如果使用 foreach 方法遍历 DataFrame 或 Dataset 的每一行数据，并对其进行处理，处理的顺序是不确定的。
    // 这是因为在 Spark 中，DataFrame 和 Dataset 的分区（Partition）是并行处理的，而分区之间的处理顺序是不确定的。
    // 因此，使用 foreach 方法遍历每一行数据时，处理的顺序也是不确定的。
    //如果需要对 DataFrame 或 Dataset 中的数据进行排序，可以使用 sort 方法对数据进行排序，并使用 collect 方法将排序结果收集到本地，然后再对排序结果进行处理。

    counts.collect().foreach(row=>println(row))

    spark.stop()

  }
}
