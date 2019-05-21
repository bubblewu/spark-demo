name := "spark-demo"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0", //% "provided",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.0.0-rc2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.1",
  "com.alibaba" % "fastjson" % "1.2.56",
  // DeepLearning4J
  "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-beta4",
  "org.deeplearning4j" %% "dl4j-spark" % "1.0.0-beta4_spark_2",
  //  "org.deeplearning4j" %% "dl4j-spark-nlp" % "1.0.0-beta4_spark_2",
  //  "org.deeplearning4j" %% "dl4j-spark-ml" % "1.0.0-beta4_spark_2",
  // nd4j: CPU版本的ND4J库为DL4J提供支持
  "org.nd4j" % "nd4j-api" % "1.0.0-beta4" exclude("com.google.guava", "guava"),
  "org.nd4j" % "nd4j-native-platform" % "1.0.0-beta4",
  // Keras
  //  "org.deeplearning4j" % "deeplearning4j-keras" % "0.9.1",
  // jieba-analysis
  "com.huaban" % "jieba-analysis" % "1.0.2",
  //  "com.hankcs" % "hanlp" % "portable-1.7.3",
  "com.google.guava" % "guava" % "27.1-jre",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"


)
