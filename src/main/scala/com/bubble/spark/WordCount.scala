package com.bubble.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词计数
  *
  * @author wu gang
  * @since 2019-03-29 22:17
  **/
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("data/word_count.txt")
    // 使用空格进行分词，得到数据为RDD[(String)]格式；
    // 通过flatMap对分割后对单词进行展平；
    // map(x => (x, 1))对每个单词进行计数为1，得到数据格式为RDD[(String, Int)]
    // reduceByKey(_ + _)根据单词也就是key进行计数,这个过程是一个Shuffle过程，数据格式为ShuffleRDD
    val wordCount = rdd.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    // 对单词统计结果RDD[(String, Int)]对键（单词）和值（词频）互换，变换后格式为RDD[(Int, String)]
    // sortByKey(false)根据键（词频）进行排序，false为倒序排列
    // 最后，再将RDD[(Int, String)]格式变换为RDD[(String, Int)]
    val wordSort = wordCount.map(x => (x._2, x._1)).sortByKey(ascending = false).map(x => (x._2, x._1))
    // 取Top 10
    wordSort.take(10)
    wordSort.saveAsTextFile("output/WordCount")
    sc.stop()
  }

}
