package com.stone.study.spark;

import static com.stone.study.spark.FileUtils.getClassPathFile;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class HelloWorldWithJava {

    public static void main(String[] args) {
        // 创建 SparkConf 对象
        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local[*]");

        // 创建 JavaSparkContext 对象
        JavaSparkContext sc = new JavaSparkContext(conf);


        // 读取文本文件
        JavaRDD<String> lines = sc.textFile(getClassPathFile("helloworld.md"));
        //    System.out.println("文件行数："+lines.count());
        //    System.out.println("文件第一行数据:"+lines.first());
        //    System.out.println("------文件内容如下------");
        //    // 对每一行数据进行处理
        //    lines.foreach(line -> {
        //        // 处理每一行数据
        //        System.out.println(line);
        //    });

        System.out.println("包含Spark的行數:"+lines.filter(line->line.contains("Spark")).count());
        //统计单词个数
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);


        // 按单词个数排序
        JavaPairRDD<Integer, String> sortedCounts = counts.mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1())).sortByKey(false);
        JavaPairRDD<String, Integer> result = sortedCounts.mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()));

        // 输出计数结果
        List<Tuple2<String, Integer>> resultList = result.collect();
        for (Tuple2<String, Integer> tuple : resultList) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }



        // 关闭 JavaSparkContext 对象
        sc.stop();
    }
}
