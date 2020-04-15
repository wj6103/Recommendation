import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.Rating
//import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS

object MovieRecommend {
  case class Movie(movieId: Int, title: String)
  case class User(userId: Int, gender: String, age: Int, occupation: Int, zipCode: String)
  def parseMovieData(data: String): Movie = {
    val dataField = data.split("::")
    assert(dataField.size == 3)
    Movie(dataField(0).toInt, dataField(1))
  }

  def parseUserData(data: String): User = {
    val dataField = data.split("::")
    assert(dataField.size == 5)
    User(dataField(0).toInt, dataField(1).toString, dataField(2).toInt, dataField(3).toInt, dataField(4).toString)
  }

  def parseRatingData(data: String): Rating = {
    val dataField = data.split("::")
    Rating(dataField(0).toInt, dataField(1).toInt, dataField(2).toDouble)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").appName("MovieRecommend").getOrCreate()
    import spark.implicits._
    val movies = spark.read.option("header","false").csv("/Users/james/Desktop/ml-25m/movies.csv")
      .map(data => {Movie(data.getAs[String](0).toInt,data.getAs[String](1))})
    val rating = spark.read.option("header","false").csv("/Users/james/Desktop/ml-25m/ratings.csv")
      .map(data => {Rating(data.getAs[String](0).toInt,data.getAs[String](1).toInt,data.getAs[String](2).toDouble)})
      .toDF("userId","movieId","rating")

//    val recomModel = new ALS().setRank(20).setIterations(10).run(rating.rdd)
//    val recomResult = recomModel.recommendProducts(162542, 10)
//    val recomUser = recomModel.recommendUsers(122916,10)
//    val movieTitles = moviesDF.as[(Int, String)].rdd.collectAsMap()
//    val recommendMoviesWithTitle = recomResult.map(rating =>(movieTitles(rating.product), rating.rating))
//    println(recommendMoviesWithTitle.mkString("\n"))
//    recomUser.foreach(println)

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
     val recomModel =  als.fit(rating)
    recomModel.setColdStartStrategy("drop")
    recomModel.save("ml.recommend.mldel")
    val recomResult = recomModel.transform(rating)
    recomResult.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(recomResult)
    println(s"Root-mean-square error = $rmse")

    val userRecs = recomModel.recommendForAllUsers(10).show(false)
    val movieRecs = recomModel.recommendForAllItems(10).show(false)

    val users = rating.select("userId").where("userId = 162542")
    val userSubsetRecs = recomModel.recommendForUserSubset(users, 10)
    userSubsetRecs.show(false)


  }
}
