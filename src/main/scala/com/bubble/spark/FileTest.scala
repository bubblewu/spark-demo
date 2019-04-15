package com.bubble.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ${DESCRIPTION}
  *
  * @author wu gang
  * @since 2019-04-11 16:08
  **/
object FileTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/Users/wugang/code/github/spark-demo/src/main/scala/com/bubble/spark/1a1xFH_2nd.asg")
    rdd.foreach(println)
    sc.stop()
  }

}
