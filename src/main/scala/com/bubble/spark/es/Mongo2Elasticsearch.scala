package com.bubble.spark.es

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark._

/**
  * 基于Spark SQL的MongoDB数据写入Elasticsearch
  *
  * @author wu gang
  * @since 2019-04-11 10:12
  **/
object Mongo2Elasticsearch {

  val MONGO_URI = "mongodb://search:search%40huoli123@123.56.222.127/tips."

  def main(args: Array[String]): Unit = {
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

    queryCreateIndexLastTime(spark)
    //        readMongoAndCreateIndex(spark)

    spark.stop()
  }

  /**
    * Spark SQL 读取MongoDB数据
    */
  def readMongoAndCreateIndex(session: SparkSession): Unit = {
    val table = "tips"
    val readConfig = ReadConfig(
      Map("collection" -> table, "readPreference.name" -> "secondaryPreferred"),
      Some(ReadConfig(session))
    )
    // 创建RDD
    val df = MongoSpark.load(session, readConfig).cache()
    //    df.printSchema()
    df.createOrReplaceTempView(table)
    // type 0=广告，1=签证，2=功略，3-抓取新闻，4=专车，5=酒店，6=公告
    // force_display 是否强制显示，-2-新入库，-1-自动，0-强制不展示，1-强制展示
    val tipsDF = session.sql("SELECT * FROM " + table + " WHERE type in (2,3,6) and force_display in (-1,1)")
    val dataSet: Dataset[String] = tipsDF.toJSON

    val tipsRdd: RDD[String] = dataSet.rdd.map(data => {
      val obj = JSON.parseObject(data)
      val id = obj.getJSONObject("_id").getString("oid")
      obj.put("id", id)
      obj.remove("_id")
      obj.put("createIndexTime", System.currentTimeMillis())
      obj.toJSONString
    })
    //    tipsRdd.foreach(println)
    tipsRdd.saveJsonToEs("huoli-rec/tips", Map("es.mapping.id" -> "id"))
  }

  def queryCreateIndexLastTime(session: SparkSession): Unit = {
    val querySort: String = "{\"sort\":[{\"createIndexTime\": {\"order\": \"desc\"}}]}"

    val query: String = "{\"query\":{\"match_all\": {}  }, \"sort\": [ { \"createIndexTime\": { \"order\": \"desc\" }  }] }"

    //    val query = "{\"query\": {\"match\" : {\"_id\": \"5bbec039b080181907b725af\"}}}"
    //    val df = session.read.format("org.elasticsearch.spark.sql")
    ////      .option("es.query", "?q=*")
    //      .load("huoli-rec")

    val df = session.read.format("es")
      //      .option("es.query", "?q=5bbec039b080181907b725af")
      .load("huoli-rec")
    df.createOrReplaceTempView("huoli")
    val dd = session.sql("SELECT * FROM huoli LIMIT 1")
    dd.printSchema()
    //    dd.select("title").show(false)
    dd.toJSON.show(false)
    dd.show(false)

    val rdd = session.sparkContext.esRDD("huoli-rec", query).first()
    println(rdd._1 + " " + rdd._2.foreach(println))

    val rdd1 = session.sparkContext.esRDD("huoli-rec", query)
    println(rdd1.collect().length)
    println(rdd1.collect().foreach(println))

  }

}
