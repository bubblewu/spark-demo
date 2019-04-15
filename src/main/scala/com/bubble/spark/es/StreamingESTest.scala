package com.bubble.spark.es

import com.bubble.spark.es.Mongo2Elasticsearch.MONGO_URI
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming-Elasticsearch集成Demo
  *
  * @author wu gang
  * @since 2019-04-10 19:09
  **/
object StreamingESTest extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Mongo2Elasticsearch")
    .config("spark.mongodb.input.uri", MONGO_URI + "tips")
    .config("spark.mongodb.output.uri", MONGO_URI + "output")
    // partitioner用于对数据进行分区的分区程序的类名.
    // MongoPaginateBySizePartitioner 创建特定数量的分区。需要查询每个分区。
    // partitioner用于对数据进行分区的分区程序的类名.
    // MongoPaginateBySizePartitioner 创建特定数量的分区。需要查询每个分区。
    .config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
    // 默认:_id.分割collection数据的字段。该字段会被索引，值唯一
    .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
    // 分区数，默认64.
    .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "127.0.0.1")
    .config("es.port", "9200")
    .config("es.nodes.wan.only", "true")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))




}
