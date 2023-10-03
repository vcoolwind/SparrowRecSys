package com.stone.study.spark

import org.apache.spark.sql.DataFrame

object DatasetStudy {
  def main(args: Array[String]): Unit = {

  }

  def agg(): Unit ={
    val df:DataFrame = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 35)
    ).toDF("Alice")

    val result = df.agg(max("age"), min("age"), avg("age"))
    result.show()
  }
}
