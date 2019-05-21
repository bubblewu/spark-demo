package com.bubble.spark.lstm.sentiment

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SparkSession
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.inputs.InputType
import org.deeplearning4j.nn.conf.layers.{EmbeddingLayer, LSTM, RnnOutputLayer}
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.nd4j.evaluation.classification.Evaluation
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.learning.config.Adam
import org.nd4j.linalg.lossfunctions.LossFunctions

import scala.collection.mutable.ArrayBuffer

/**
  * 基于DeepLearning4J + Spark环境的LSTM文本情感分类
  *
  * @author wu gang
  * @since 2019-05-20 15:35
  **/
object SentimentLSTM {


  def getTrainData(spark: SparkSession): (JavaRDD[DataSet], Int) = {
    """
      | 读取csv并进行分词形成需要的JavaRDD
    """.stripMargin
    val basePath = "/Users/wugang/datasets/nlp/sentiment_classification/sentiment/"
    // 好评
    val posData = spark.read.format("csv").load(basePath + "pos.xls").toDF("describe")
    println("正向情感数据共：" + posData.count())
    // 差评
    val negData = spark.read.format("csv").load(basePath + "neg.xls").toDF("describe")
    println("负向情感数据共：" + negData.count())

    // 设置正则
    val regEx =
      """[`~!@#$%^&*()+=|{}':;',『』[\-]《》\\[\\][\"][ ]\[\][0123456789].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"""
    val signPattern = regEx.r
    val targetColumns = posData.columns
//    @transient val segmenter = new JiebaSegmenter
    // 在rdd中对评论进行分词并给出标签
    val posRDD = posData.select(targetColumns.head, targetColumns.tail: _*).rdd.map(x => {
      val segmenter = new JiebaSegmenter
      val word: String = x(0).asInstanceOf[String]
      val wordSplit = segmenter.sentenceProcess(signPattern.replaceAllIn(word.trim, "")).toArray.mkString(" ")
      ("正面", wordSplit)
    }).filter(row => row._2.length > 0)
    val negRDD = negData.select(targetColumns.head, targetColumns.tail: _*).rdd.map(x => {
      val word: String = x(0).asInstanceOf[String]
      val segmenter = new JiebaSegmenter
      val wordSplit = segmenter.sentenceProcess(signPattern.replaceAllIn(word.trim, "")).toArray.mkString(" ")
      ("反面", wordSplit)
    }).filter(row => row._2.length > 0)
    //汇总
    val totalRDD = posRDD.union(negRDD)
    println("正负情感数据共：" + totalRDD.count())

    // 得到词的统计并按照个数进行降序排列用于生成一个词对index的map
    val wordIndexMap: Map[String, Int] = {
      val vocab = totalRDD.flatMap(x => x._2.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .map(row => row._1)
        .collect()
      vocab.zipWithIndex.toMap
    }
    // 将最大的数目设为200，此处需要说明一下，本来最大数目约为1500，
    // 但笔者在尝试时一直报内存溢出的错误，分析下来确实是1500的词长度超出了内存的限制，
    // 所以考虑到大部分词句的长度都在100左右，因此设置了两百作为最大长度
    //    val maxCorpusLength = totalRDD.map(row => row._2.split(" ").length).collect().max
    val maxCorpusLength = 200
    // 词袋的大小，50848
    val vocabSize = totalRDD.flatMap(x => x._2.split(" ")).collect.distinct.length
    println("词袋的大小: " + vocabSize)
    // 标签，只有两个
    val labelWord = totalRDD.flatMap(x => x._1.split(" ")).collect.distinct

    // 生成最终的JavaRDD[DataSet]，此处的DataSet并不是Spark的DataSet，而是nd4j的DataSet，实际上是向量形式。
    // 有两种格式，一种是只含input和output两个张量，另一个则加上了label mask和features mask，用于说明对应位置的label和features是否考虑，
    // 具体可见wangongxi的文章解释，也可以直接goto到类下面看函数解释
    // 此处参考wangongxi的文章设置，给了features和label的mask表示，但笔者的词语最大长度为200
    val totalDataSet = totalRDD.map(row => {
      //      val listWords = totalRDD.take(1)(0)._2.split(" ")
      val listWords = if (row._2.split(" ").length >= 200) row._2.split(" ").take(200) else row._2.split(" ")
      //      val label = totalRDD.take(1)(0)._1
      val label = row._1
      val features: INDArray = Nd4j.create(1, 1, maxCorpusLength)
      val labels = Nd4j.create(1, labelWord.length, maxCorpusLength)
      val featuresMask = Nd4j.zeros(1.toLong, maxCorpusLength.toLong)
      val labelsMask = Nd4j.zeros(1.toLong, maxCorpusLength.toLong)
//      println("features mask shape: " + labelsMask.shape())
//      println("labels mask shape: " + labelsMask.shape())

      val origin = new Array[Int](3)
      val mask = new Array[Int](2)
      var i: Int = 0
      for (word <- listWords) {
        // 标量
        features.putScalar(Array(1, 1, i), wordIndexMap(word))
        featuresMask.putScalar(Array(0, i), 1)
        i += 1
      }
      val lastIdx: Int = listWords.size
      val idx = labelWord.indexOf(label)
      labels.putScalar(Array[Int](0, idx, lastIdx - 1), 1.0)
      labelsMask.putScalar(Array[Int](0, lastIdx - 1), 1.0)
      new DataSet(features, labels, featuresMask, labelsMask)
    })
    (totalDataSet.toJavaRDD(), vocabSize)
  }

  def buildModel(sc: SparkContext, vocabSize: Int): SparkDl4jMultiLayer = {
    """
      | 设置LSTM的网络层，词袋尺寸为50835，每个词在第2步会被替换成对应的序号，例如最多的"的"，会变成0，
      | 依次类推，然后再进入LSTM的layer0，即embeding层，进行embeding，将每个词由序号变为256维的向量，
      | 接着进入GravesLSTM层和RNN输出层
    """.stripMargin

    val conf = {
      new NeuralNetConfiguration.Builder()
        .seed(1234)
        .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
        .updater(Adam.builder().learningRate(1e-4).beta1(0.9).beta2(0.999).build())
        .l2(5 * 1e-4)
        .list()
        .layer(0, new EmbeddingLayer.Builder().nIn(vocabSize).nOut(256).activation(Activation.IDENTITY).build())
        .layer(1, new LSTM.Builder().nIn(256).nOut(256).activation(Activation.SOFTSIGN).build())
        .layer(2, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT).activation(Activation.SOFTMAX).nIn(256).nOut(2).build())
        .setInputType(InputType.recurrent(vocabSize))
        .build()
    }

    val examplesPerDataSetObject = 1
    val averagingFrequency: Int = 5
    val batchSizePerWorker: Int = 20

    val trainMaster = {
      new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
        .workerPrefetchNumBatches(0)
        .saveUpdater(true)
        .averagingFrequency(averagingFrequency)
        .batchSizePerWorker(batchSizePerWorker)
        .build()
    }
    val sparkNetwork: SparkDl4jMultiLayer = new SparkDl4jMultiLayer(sc, conf, trainMaster)
    sparkNetwork
  }


  def trainLSTM(sparkNetwork: SparkDl4jMultiLayer, emotionWordData: JavaRDD[DataSet]): Unit = {
    """
      | 进行训练，训练10个循环，将训练数据和测试数据按照0.5/0.5分配，每个训练循环结束给出对训练数据和测试数据的预测准确率。
    """.stripMargin

    var numEpoch = 0
    // 训练集和测试集分割
    val Array(trainingData, testingData) = emotionWordData.randomSplit(Array(0.5, 0.5))
    println("训练集大小: " + trainingData.count())
    println("测试集大小: " + testingData.count())

    val resultArray = new ArrayBuffer[Array[String]](0)
    for (numEpoch <- 1 to 10) {
      sparkNetwork.fit(trainingData)
      val trainEvaluation: Evaluation = sparkNetwork.evaluate(trainingData)
      val trainAccuracy = trainEvaluation.accuracy()
      val testEvaluation: Evaluation = sparkNetwork.evaluate(testingData)
      val testAccuracy = testEvaluation.accuracy()

      System.out.println("====================================================================")
      System.out.println("Epoch " + numEpoch + " Has Finished")
      System.out.println("Train Accuracy: " + trainAccuracy)
      System.out.println("Test Accuracy: " + testAccuracy)
      System.out.println("====================================================================")
      resultArray.append(Array(trainAccuracy.toString, testAccuracy.toString))
    }

//    val network = sparkNetwork.getNetwork()
//    val data = Vectors.dense(1.0,2.0,3.0)
//    sparkNetwork.predict(data)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("Spark LSTM Emotion Analysis")
        .getOrCreate()
    }
    val sc = JavaSparkContext.fromSparkContext(spark.sparkContext)
    sc.setLogLevel(logLevel = "WARN")
    val (emotionWordData: JavaRDD[DataSet], vocabSize: Int) = getTrainData(spark)
    val sparkNetwork = buildModel(sc, vocabSize)
    trainLSTM(sparkNetwork, emotionWordData = emotionWordData)
  }

}
