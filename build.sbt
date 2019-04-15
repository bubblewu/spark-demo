name := "spark-demo"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0", //% "provided",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.0.0-rc2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.1",
  "com.alibaba" % "fastjson" % "1.2.56"
)
