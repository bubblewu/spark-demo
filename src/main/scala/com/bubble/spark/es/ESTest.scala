package com.bubble.spark.es

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Spark-Elasticsearch集成Demo
  *
  * @author wu gang
  * @since 2019-04-10 17:07
  **/

case class Trip(departure: String, arrival: String)

object ESTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("ESTest").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")
    val sc = new SparkContext(conf)

    //    write(sc)
    //    writeClass(sc)
    //    writeJson(sc)
    //        writeByResource(sc)
    //    writeWithMetadata(sc)

    read(sc)
    sc.stop()
  }

  def write(sc: SparkContext): Unit = {
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    // Index为spark/docs
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
  }


  def writeClass(sc: SparkContext): Unit = {
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "spark/docs")
    //    EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))
  }

  def writeJson(sc: SparkContext): Unit = {
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    val rdd = sc.makeRDD(Seq(json1, json2))
    rdd.saveJsonToEs("spark/docs")
  }

  /**
    * Writing to dynamic/multi-resources
    */
  def writeByResource(sc: SparkContext): Unit = {
    val game = Map(
      "media_type" -> "game",
      "title" -> "FF VI",
      "year" -> "1994")
    val book = Map("media_type" -> "book", "title" -> "Harry Potter", "year" -> "2010")
    val cd = Map("media_type" -> "music", "title" -> "Surfing With The Alien")
    //    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/docs")

    // 指定唯一标识_id
    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/docs", Map("es.mapping.id" -> "title"))
  }

  def writeWithMetadata(sc: SparkContext): Unit = {
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    // metadata for each document
    // note it's not required for them to have the same structure
    //    val otpMeta = Map(ID -> 1, TTL -> "3h")
    //    val mucMeta = Map(ID -> 2, VERSION -> "23")
    //    val sfoMeta = Map(ID -> 3)
    //    val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))

    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    airportsRDD.saveToEsWithMeta("airports/2015")
  }

  def read(sc: SparkContext): Unit = {
    val query: String =
      s"""{"query" : {"match_all" : {}}, "filter" : {"term" : {"type" : "4"}}}"""
    val shouldQuery: String =
      s"""{"query" : {"bool" : {"should" : [{"match": {"_id":"5bbec039b080181907b725af"}}, {"match": {"locations":"LON"}}]}}}"""
    val mustQuery: String =
      s"""{"query" : {"bool" : {"must" : [{"match": {"title":"伦敦"}}, {"match": {"locations":"LON"}}]}}}"""
    val rdd = sc.esRDD("huoli-rec", mustQuery)
    println(rdd.collect().toBuffer)
  }


}
