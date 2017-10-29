name := "spark-sentiment-analysis"

version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0",

  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)