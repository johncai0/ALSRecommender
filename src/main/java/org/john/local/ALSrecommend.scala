package org.john.local

import org.apache.spark.sql.{DataFrame, SparkSession}

class ALSrecommend(ss:SparkSession, df:DataFrame) extends Serializable {
  import org.apache.spark.broadcast.Broadcast
  import org.apache.spark.ml.recommendation.{ALS, ALSModel}
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  import ss.implicits._

  import scala.collection.mutable.ArrayBuffer
  import scala.util.Random
  val userandid=df.select("user").rdd.map(_.getString(0)).distinct().zipWithIndex().map(l=>(l._1,l._2.toInt)).toDF("user","userid")
  val itemandid=df.select("item").rdd.map(_.getString(0)).distinct().zipWithIndex().map(l=>(l._1,l._2.toInt)).toDF("item","itemid")
  val alldata=df.join(userandid,"user").join(itemandid,"item")
  var gauc:Double=0.00
  var rank:Int=0
  var regParam:Double=0.00
  var alpha:Double=0.00
  var goodModel:ALSModel=null
  def evaluate(): Unit = {
    println(s"the count users: ${userandid.count()}")
    println(s"the count tags: ${itemandid.count()}")
    //val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
    val Array(trainData, cvData) = alldata.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = itemandid.select("itemid").as[Int].collect()
    val bAllItemIDs = ss.sparkContext.broadcast(allItemIDs)


    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(trainData))
    println(s"the stat auc: ${mostListenedAUC}")

    val evaluations =
      for (rank     <- Seq(10,  11);
           regParam <- Seq(3.0, 2.0);
           alpha    <- Seq(8.0, 1.0))
        yield {
          val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(rank).setRegParam(regParam).
            setAlpha(alpha).setMaxIter(20).
            setUserCol("userid").setItemCol("itemid").
            setRatingCol("itemrank").setPredictionCol("prediction").
            fit(trainData)

          val auc = areaUnderCurve(cvData, bAllItemIDs, model.transform)


          if (auc > gauc) {
            goodModel=model
          } else {
            model.userFactors.unpersist()
            model.itemFactors.unpersist()
          }
          (auc, (rank, regParam, alpha))
        }
    evaluations.sorted.reverse.foreach(l=>{
      println(l)
      if (l._1>gauc) {
        rank=l._2._1
        regParam=l._2._2
        alpha=l._2._3
      }
    })
    trainData.unpersist()
    cvData.unpersist()
  }
  def areaUnderCurve(positiveData: DataFrame, bAllItemIDs: Broadcast[Array[Int]], predictFunction: (DataFrame => DataFrame)): Double = {
    val positivePredictions = predictFunction(positiveData.select("userid", "itemid")).
      withColumnRenamed("prediction", "positivePrediction")

    val negativeData = positiveData.select("userid", "itemid").as[(Int,Int)].
      groupByKey { case (user, _) => user }.
      flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, item) => item }.toSet
        val negative = new ArrayBuffer[Int]()
        val allItemIDs = bAllItemIDs.value
        var i = 0

        while (i < allItemIDs.length && negative.size < posItemIDSet.size) {
          val itemID = allItemIDs(random.nextInt(allItemIDs.length))
          // Only add new distinct IDs
          if (!posItemIDSet.contains(itemID)) {
            negative += itemID
          }
          i += 1
        }
        // Return the set with user ID added back
        negative.map(itemID => (userID, itemID))
      }.toDF("userid", "itemid")

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeData).
      withColumnRenamed("prediction", "negativePrediction")

    val joinedPredictions = positivePredictions.join(negativePredictions, "userid").
      select("userid", "positivePrediction", "negativePrediction").cache()

    // Count the number of pairs per user
    val allCounts = joinedPredictions.
      groupBy("userid").agg(count(lit("1")).as("total")).
      select("userid", "total")
    // Count the number of correctly ordered pairs per user
    val correctCounts = joinedPredictions.
      filter($"positivePrediction" > $"negativePrediction").
      groupBy("userid").agg(count("userid").as("correct")).
      select("userid", "correct")

    // Combine these, compute their ratio, and average over all users
    val meanAUC = allCounts.join(correctCounts, Seq("userid"), "left_outer").
      select($"userid", (coalesce($"correct", lit(0)) / $"total").as("auc")).
      agg(mean("auc")).
      as[Double].first()

    joinedPredictions.unpersist()

    meanAUC
  }

  def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts = train.groupBy("itemid").
      agg(sum("itemrank").as("prediction")).
      select("itemid", "prediction")
    allData.
      join(listenCounts, Seq("itemid"), "left_outer").
      select("userid", "itemid", "prediction")
  }
  def makeRecommendations(user: String, howMany: Int): DataFrame = {
    val userID = this.userandid.where(s"user = '" + user + "'").select("userid").rdd.collect()(0).getInt(0)
    val toRecommend = goodModel.itemFactors.
      select($"id".as("itemid")).
      withColumn("userid", lit(userID))
    if (goodModel == null && goodModel ==None) println("goodModel is Null.........")
    val reced=goodModel.transform(toRecommend).
      select("itemid", "prediction").
      orderBy($"prediction".desc).
      limit(howMany)
    reced.join(itemandid,"itemid").drop("itemid").sort("prediction")
  }
}
