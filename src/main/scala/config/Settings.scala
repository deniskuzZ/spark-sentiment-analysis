package config
import com.typesafe.config.ConfigFactory

/**
  * Created by kuzmende on 10/14/17.
  */
object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("clickstream")

    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val pages = weblogGen.getInt("pages")
    lazy val visitors = weblogGen.getInt("visitors")
    lazy val numberOfFiles = weblogGen.getInt("number_of_files")

    lazy val kafkaTopic = weblogGen.getString("kafka_topic")
    lazy val kafkaOffsets = weblogGen.getString("kafka_offsets")

    lazy val hdfsPath = weblogGen.getString("hdfs_path")
  }
}

