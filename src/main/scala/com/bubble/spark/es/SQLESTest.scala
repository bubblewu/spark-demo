package com.bubble.spark.es

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * Spark SQL-Elasticsearch集成Demo}
  *
  * @author wu gang
  * @since 2019-04-10 19:09
  **/
object SQLESTest {

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
      .getOrCreate()

    readBySchema(spark)

    spark.stop()
  }

  /**
    * Spark SQL 读取MongoDB数据
    */
  def read(spark: SparkSession): Unit = {
    val table = "trip_subscribe"
    val readConfig = ReadConfig(
      Map("collection" -> table, "readPreference.name" -> "secondaryPreferred"),
      Some(ReadConfig(spark))
    )
    // 创建RDD
    val df = MongoSpark.load(spark, readConfig)
    df.show(true)

    println("----------")
    df.createOrReplaceTempView(table)
    val resDf = spark.sql("select _id,tripKey from " + table)
    resDf.show()
  }

  /**
    * 使用Schema约束
    */
  def readBySchema(spark: SparkSession): Unit = {
    val table = "trip_subscribe"
    val schema = StructType(
      List(
        StructField("_id", StringType),
        StructField("tripKey", StringType),
        StructField("addTime", LongType)
      )
    )
    val readConfig = ReadConfig(
      Map("collection" -> table, "readPreference.name" -> "secondaryPreferred"),
      Some(ReadConfig(spark))
    )
    // 通过schema约束，直接获取需要的字段
    val df = spark.read.format("com.mongodb.spark.sql").schema(schema).options(readConfig.asOptions).load()
    df.printSchema()
    df.show(true)

    println("----------")
    df.createOrReplaceTempView(table)
    val resDf = spark.sql("select * from " + table)

    val rdd = resDf.toJSON
    rdd.show(true)
    //    resDf.show()
  }

}
