//import MovieRecommendCollaborativeFiltering.Movie
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.ml.recommendation.ALSModel
//import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
//import org.apache.spark.sql.{Row, SparkSession}
//
//import scala.collection.mutable
//
//object CollaborativeFiltering {
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val spark = SparkSession.builder.master("local").appName("MovieRecommend").getOrCreate()
//    import spark.implicits._
//    val movies = spark.read.option("header","false").csv("/Users/james/Desktop/ml-25m/movies.csv")
//      .map(data => {Movie(data.getAs[String](0).toInt,data.getAs[String](1))}).cache()
//    val moviesDF = movies.toDF()
//    val rating = spark.read.option("header","false").csv("/Users/james/Desktop/ml-25m/ratings.csv")
//      .map(data => {Rating(data.getAs[String](0).toInt,data.getAs[String](1).toInt,data.getAs[String](2).toDouble)})
//      .toDF("userId","movieId","rating")
////    RecommendMovie(spark,162542,20)
//    val recomModel = ALSModel.load("/Users/james/IdeaProjects/recommendation/ml.recommend.mldel")
//    val users = rating.select("userId").where("userId = 162542")
//    val reResult = recomModel.recommendForUserSubset(users,5)
//
//    val movieTitles = moviesDF.as[(Int, String)].rdd.collectAsMap()
//    val recommendMoviesId = reResult.rdd.map(row => {
//      val user_id = row.getInt(0)
//      val recs = row.getAs[mutable.WrappedArray[Row]](1)
////      var userRecommendatinos = new Array[String](5)
////      for (rec <- recs) {
////        val item_id = rec.getInt(0)
////        userRecommendatinos += movieTitles(item_id)
////      }
//      var movielist = ""
//      recs.foreach(x => {
//        movielist += movieTitles(x.getInt(0))+","
//      }
//
//      )
//      (user_id,movielist.substring(0,movielist.length))
//    })
//  recommendMoviesId.foreach(x => println(x))
//
//
//  }
//  def RecommendMovie(spark: SparkSession, userId : Int, number : Int): Unit ={
//
//    import spark.implicits._
//    val movies = spark.read.option("header","false").csv("/Users/james/Desktop/ml-25m/movies.csv")
//      .map(data => {Movie(data.getAs[String](0).toInt,data.getAs[String](1))}).cache()
//    val moviesDF = movies.toDF()
//
//    val recommendModel = MatrixFactorizationModel.load(spark.sparkContext,"/Users/james/IdeaProjects/recommendation/RecommendModel.model")
//    val reResult = recommendModel.recommendProducts(userId,number)
//
//    val movieTitles = moviesDF.as[(Int, String)].rdd.collectAsMap()
//    val recommendMoviesWithTitle = reResult.map(rating =>(movieTitles(rating.product), rating.rating))
//    println(recommendMoviesWithTitle.mkString("\n"))
//
//  }
//}
