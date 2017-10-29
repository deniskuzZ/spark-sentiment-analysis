package utils

import config.Settings
import org.apache.spark.sql.SparkSession

/**
  * Created by kuzmende on 10/16/17.
  */
object SparkUtils {

  def getSparkSession(appName: String) = {
    var checkpointDirectory = Settings.WebLogGen.hdfsPath

    val spark = SparkSession.builder
      .master("local[*]")
      .appName(appName)
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir(checkpointDirectory)

    spark
  }

}
