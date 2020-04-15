import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MovieRecommendContentBasedFiltering {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").appName("MovieRecommend").getOrCreate()
    import spark.implicits._
    val movie = spark.read.option("header",true).csv("/Users/james/Desktop/tmdb-movie-metadata/tmdb_5000_movies.csv")
        .drop("budget").drop("genres").drop("homepage").drop("original_language").drop("original_title")
      .drop("overview").drop("popularity").drop("production_companies").drop("production_countries").drop("release_date")
      .drop("revenue").drop("runtime").drop("spoken_languages").drop("tagline")
      .drop("vote_count").drop("tagline").drop("status")
    movie.show(false)
  }
}
