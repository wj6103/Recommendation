//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql._
//import org.apache.spark.mllib.recommendation.Rating
//import org.apache.spark.ml.recommendation.ALS
//import org.apache.spark.rdd.RDD
//import org.elasticsearch.spark._
//
//
//object MovieRecommendCollaborativeFiltering {
//  def main(args: Array[String]) {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val spark = SparkSession.builder.master("local").appName("MovieRecommend")
//      .config("es.index.auto.create", "true")
//      .config("es.nodes.wan.only","false")
//      .config("es.nodes", "10.205.48.10")
//      .config("es.port", "9200")
//      .getOrCreate()
//    import spark.implicits._
//    val userRating = spark.sparkContext.esRDD("rating/rating").map(x => {
//      val data = x._2
//      val userId = data.get("userId").get.asInstanceOf[Long]
//      val movieId = data.get("movieId").get.asInstanceOf[Long]
//      val rating = data.get("rating").get.asInstanceOf[Float]
//      (userId,movieId,rating)
//    }).toDF("userId","movieId","rating")
//    userRating.show()
//
////    val als = new ALS()
////      .setMaxIter(5)
////      .setRegParam(0.01)
////      .setUserCol("userId")
////      .setItemCol("movieId")
////      .setRatingCol("rating")
////     val recomModel =  als.fit(userRating)
////    recomModel.setColdStartStrategy("drop")
////    recomModel.save("ALSPornRecommendationModel")
////    val recomResult = recomModel.transform(userRating)
////    recomResult.show()
//
////    val evaluator = new RegressionEvaluator()
////      .setMetricName("rmse")
////      .setLabelCol("rating")
////      .setPredictionCol("prediction")
////    val rmse = evaluator.evaluate(recomResult)
////    println(s"Root-mean-square error = $rmse")
////
////    val userRecs = recomModel.recommendForAllUsers(10).show(false)
////    val movieRecs = recomModel.recommendForAllItems(10).show(false)
////
////    val users = rating.select("userId").where("userId = 162542")
////    val userSubsetRecs = recomModel.recommendForUserSubset(users, 10)
////    userSubsetRecs.show(false)
//
//
//  }
//}
