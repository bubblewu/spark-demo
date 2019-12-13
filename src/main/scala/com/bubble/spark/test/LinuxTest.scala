package com.bubble.spark.test

import scala.sys.process._

/**
  * 执行Linux命令
  *
  * @author wu gang
  * @since 2019-04-11 17:55
  **/
object LinuxTest {

  def main(args: Array[String]): Unit = {
    delESIndex()
  }

  def test(): Unit = {
    val cmd = s"rm /Users/wugang/data/logs/segment/2019-02/segment-2019-02-11.log"
    println("execute shell: " + cmd)
    s"$cmd".!
  }

  def delESIndex(): Unit = {
    val time = 1555040343407L
//    val delIndexByTimeCmd = "curl --request POST " +
//      "--url 'http://127.0.0.1:9200/huoli-rec/tips/_delete_by_query?pretty=' \n" +
//      "--header 'Content-Type: application/json' \n " +
//      "--header 'Postman-Token: c861e4ac-fce5-4735-8791-4aa21863fa0f' \n " +
//      "--header 'cache-control: no-cache' \n  " +
//      "--data '{\"query\": {\"bool\": {\"filter\": {\"range\": {\"createIndexTime\": {\"lte\": $time}}}}}}'"
//    println("execute shell: " + delIndexByTimeCmd)
//    s"$delIndexByTimeCmd".!


    val delShell = s"sh /Users/wugang/code/github/spark-demo/src/main/scala/com/bubble/spark/es/delIndexByTime.sh $time"
    println("execute shell: " + delShell)
    s"$delShell".!
  }

}
