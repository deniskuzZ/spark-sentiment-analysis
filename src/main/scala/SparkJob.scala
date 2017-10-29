import config.Settings
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import utils.SparkUtils.getSparkSession

import scala.util.Try

/**
  * Created by kuzmende on 10/16/17.
  */
object SparkJob extends App {

  // setup spark context
  val spark = getSparkSession("clickstream-analytics")

  val wlc = Settings.WebLogGen

  // initialize input RDD
  val tweetDF = spark.read.json("src/main/resources/tweets/*")
  tweetDF.show()

  // clean records
  var tweets = cleanRecords(tweetDF)

  // label records
  val labeledTweets = labelData(tweets)
  labeledTweets.take(5).foreach(x => println(x))

  val hashingTF = new HashingTF(2000)

  // transform data
  val input_labeled = extractFeatures(hashingTF, labeledTweets)
  input_labeled.take(5).foreach(println)

  // keep the raw text for inspection later
  var sample = labeledTweets.take(1000).map(
    t => (t._1, hashingTF.transform(t._2), t._2))
    .map(x => (new LabeledPoint(x._1.toDouble, x._2), x._3))

  val splits = input_labeled.randomSplit(Array(0.7, 0.3))
  val (trainingData, validationData) = (splits(0), splits(1))

  val model = buildModel(trainingData)

  // Evaluate model on test instances and compute test error
  var labelAndPredsTrain = evaluateModel(model, trainingData)
  var labelAndPredsValid = evaluateModel(model, validationData)

  validate(labelAndPredsTrain, trainingData, "Training")
  validate(labelAndPredsValid, validationData, "Validation")

  val predictions = sample.map { point =>
    val prediction = model.predict(point._1.features)
    (point._1.label, prediction, point._2)
  }

  predictions.take(100).foreach(x =>
    println("label: " + x._1 + " prediction: " + x._2 + " text: " + x._3.mkString(" ")))

  //model.save(spark.sparkContext, "hdfs:///tmp/tweets/RandomForestModel")


  def cleanRecords(tweetDF: DataFrame): DataFrame = {
    var messages = tweetDF.select("msg")
    println("Total messages: " + messages.count())

    var happyMessages = messages.filter(messages("msg").contains("happy"))
    val countHappy = happyMessages.count()
    println("Number of happy messages: " + countHappy)

    var unhappyMessages = messages.filter(messages("msg").contains(" sad"))
    val countUnhappy = unhappyMessages.count()
    println("Unhappy Messages: " + countUnhappy)

    val smallest = Math.min(countHappy, countUnhappy).toInt

    //Create a dataset with equal parts happy and unhappy messages
    happyMessages.limit(smallest).union(unhappyMessages.limit(smallest))
  }

  def labelData(tweets: DataFrame): RDD[(Int, Seq[String])] = {
    val messagesRDD = tweets.rdd

    val goodBadRecords = messagesRDD.map(
      row => {
        Try {
          val msg = row(0).toString.toLowerCase()
          var isHappy: Int = 0

          if (msg.contains(" sad")) {
            isHappy = 0

          } else if (msg.contains("happy")) {
            isHappy = 1
          }

          var msgSanitized = msg.replaceAll("happy", "")
          msgSanitized = msgSanitized.replaceAll("sad", "")

          (isHappy, msgSanitized.split(" ").toSeq)
        }
      }
    )

    //We use this syntax to filter out exceptions
    val exceptions = goodBadRecords.filter(_.isFailure)
    println("total records with exceptions: " + exceptions.count())
    exceptions.take(10).foreach(x => println(x.failed))

    var labeledTweets = goodBadRecords.filter(_.isSuccess).map(_.get)
    println("total records with successes: " + labeledTweets.count())
    labeledTweets
  }

  def extractFeatures(hashingTF: HashingTF, labeledTweets: RDD[(Int, Seq[String])]): RDD[LabeledPoint] = {
    //Map the input strings to a tuple of labeled point + input text
    labeledTweets.map(
      t => (t._1, hashingTF.transform(t._2)))
      .map(x => new LabeledPoint(x._1.toDouble, x._2))
  }

  def buildModel(trainingData: RDD[LabeledPoint]): GradientBoostedTreesModel = {
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")

    //number of passes over our training data
    boostingStrategy.setNumIterations(20)

    //We have two output classes: happy and sad
    boostingStrategy.treeStrategy.setNumClasses(2)

    //Depth of each tree. Higher numbers mean more parameters, which can cause overfitting.
    boostingStrategy.treeStrategy.setMaxDepth(5)

    GradientBoostedTrees.train(trainingData, boostingStrategy)
  }

  def evaluateModel(model: GradientBoostedTreesModel, data: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    data.map { point =>
      val prediction = model.predict(point.features)
      Tuple2(point.label, prediction)
    }
  }

  private def validate(labelAndPreds: RDD[(Double, Double)], data: RDD[LabeledPoint], setType: String) = {
    val results = labelAndPreds.collect()

    var happyTotal = 0
    var unhappyTotal = 0
    var happyCorrect = 0
    var unhappyCorrect = 0

    results.foreach(
      r => {
        if (r._1 == 1) {
          happyTotal += 1
        } else if (r._1 == 0) {
          unhappyTotal += 1
        }
        if (r._1 == 1 && r._2 == 1) {
          happyCorrect += 1
        } else if (r._1 == 0 && r._2 == 0) {
          unhappyCorrect += 1
        }
      }
    )
    println("unhappy messages in " + setType + " Set: " + unhappyTotal + " happy messages: " + happyTotal)
    println("happy % correct: " + happyCorrect.toDouble / happyTotal)
    println("unhappy % correct: " + unhappyCorrect.toDouble / unhappyTotal)

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / data.count()
    println("Test Error " + setType + " Set: " + testErr)
  }

}

