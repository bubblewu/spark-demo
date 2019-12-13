package com.bubble.spark.classify

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * 基于SparkML的NaiveBayes算法，针对中文文本数据（搜狗数据集）进行分类
  *
  * @author wu gang
  * @since 2019-06-26 17:35
  **/
object NBDemo {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]): Unit = {
    // 创建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("NaiveBayes Demo").master("local[*]").getOrCreate()
    // 获取sparkContext
    val sc: SparkContext = sparkSession.sparkContext

    // 设置日志级别
    sc.setLogLevel("WARN")

    val sqlContext = new SQLContext(sc)


//    val df = sparkSession.read.textFile("/Users/wugang/datasets/classify/sougou-train").as[RawDataRecord]
//    df.show()
//    var rr = df.toDF().rdd.map(
//      case Row(key: String, value: String) => println(key)
//    })

    var rdd = sc.textFile("/Users/wugang/datasets/classify/sougou-train").map(
      line => {
        var data = line.split(",")
        RawDataRecord(data(0), data(1))
      }
    )

//    rdd.toDS().show(2)


//    // 70%作为训练数据，30%作为测试数据
//    val splits = rdd.randomSplit(Array(0.7, 0.3))
//    var trainDF = sparkSession.createDataFrame(splits(0), RawDataRecord.getClass)
//    var testDF = sparkSession.createDataFrame(splits(1), RawDataRecord.getClass)
//
//    testDF.show(truncate = false)
//    // 将词语转换成数组
//    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//    var wordsData = tokenizer.transform(trainDF)
//    println("output1：")
//    wordsData.select("category", "text", "words").take(1)
//    wordsData.select("category", "text", "words").map(x => {
//      println(x)
//    })

//    // 计算每个词在文档中的词频
//    var hashingTF = new HashingTF().setNumFeatures(500000)
//      .setInputCol("words").setOutputCol("rawFeatures")
//    var featurizedData = hashingTF.transform(wordsData)
//    println("output2：")
//    featurizedData.select("category", "words", "rawFeatures").take(1)
//
//    // 计算每个词的TF-IDF
//    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    var idfModel = idf.fit(featurizedData)
//    var rescaledData = idfModel.transform(featurizedData)
//    println("output3：")
//    rescaledData.select("category", "features").take(1)
//
//    // 转换成Bayes的输入格式
//    var trainDataRdd = rescaledData.select("category", "features").map {
//      case Row(label: String, features: Vector[Double]) => LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }
//    println("output4：")
//    trainDataRdd.take(1)
//
//    // 训练模型
//    val model = new NaiveBayes().setModelType("multinomial").fit(trainDataRdd)
////    val model = new NaiveBayes().train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
//
//
//    // 测试数据集，做同样的特征表示及格式转换
//    var testwordsData = tokenizer.transform(testDF)
//    var testfeaturizedData = hashingTF.transform(testwordsData)
//    var testrescaledData = idfModel.transform(testfeaturizedData)
//    var testDataRdd = testrescaledData.select("category", "features").map {
//      case Row(label: String, features: Vector[Double]) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }
//    // 对测试数据集使用训练模型进行分类预测
//    val testpredictionAndLabel = testDataRdd.map(p => (model.transform(testDataRdd), p.label))
//
//
//    //统计分类准确率
//    var test_accuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    println("output5：")
//    println(test_accuracy)
  }

}
