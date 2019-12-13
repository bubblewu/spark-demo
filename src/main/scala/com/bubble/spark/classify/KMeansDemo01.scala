package com.bubble.spark.classify

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于Spark对数据进行K-Means聚类
  *
  * @author wu gang
  * @since 2019-07-05 10:53
  **/
object KMeansDemo01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("K-Means Demo01").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val data = sc.textFile("/Users/wugang/code/github/spark-demo/src/main/scala/com/bubble/spark/classify/score.txt")
    // 将文本文件的内容转化为 Double 类型的 Vector 集合
    val train_data = data.map(line => Vectors.dense(line.split(" ").map(_.toDouble)))
    // 由于 K-Means 算法是迭代计算，这里把数据缓存起来（广播变量）
    train_data.cache()
    // 分为 3 个子集，最多50次迭代
    val model = KMeans.train(train_data, 3, 50)
    // 输出每个子集的质心
    model.clusterCenters.foreach(println)

    val modelCost = model.computeCost(train_data)
    // 输出本次聚类操作的收敛性，此值越低越好
    println("K-Means Cost:" + modelCost)

    // 输出每组数据及其所属的子集索引
    train_data.foreach(d => println("label-" + model.predict(d) + " : " + d))

    sc.stop()
  }


}
