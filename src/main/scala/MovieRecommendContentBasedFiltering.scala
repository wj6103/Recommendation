import java.util.Calendar
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Word2Vec}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import org.slf4j
import org.slf4j.LoggerFactory
import scala.collection.mutable

object MovieRecommendContentBasedFiltering {
  private val logger: slf4j.Logger = LoggerFactory.getLogger(MovieRecommendContentBasedFiltering.getClass)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("MovieRecommend")//.master("local[*]")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryoserializer.buffer.max","512")
      .config("es.index.auto.create", "true")
      .config("es.nodes.wan.only","false")
      .config("es.nodes", "10.205.48.10")
      .config("es.port", "9200")
      .getOrCreate()

    import spark.implicits._
    val filePath = "file:///Users/james/Desktop/pornhub.json"
    val hdfsPath = "/user/james/pornhub.json"
    val porn = spark.read.json(hdfsPath).filter(size($"tags")>0).withColumn("id",monotonically_increasing_id)
      //.select("id","title","actors","categories","tags","watch","like","image","video_url","published_time")
      .toDF()

    val w2v = new Word2Vec()
      .setInputCol("tags")
      .setOutputCol("features")
      .setMaxIter(5)
      .setVectorSize(300)
      .setMinCount(1)
      .fit(porn)
    val w2vResult = w2v.transform(porn)

    val compare = w2vResult.select("id","features").map(a => (a.getLong(0),a.getAs[DenseVector](1).toArray)).cache()
    val comArray = compare.collect()
    val comRdd = compare.rdd//.repartition(args(0).toInt)

    val br_c = spark.sparkContext.broadcast(comArray)

    val r = comRdd.map(rdd => {
      val recommend = br_c.value.map(d => {
        val sim = cosineSimilarity(rdd._2, d._2)
        (d._1,sim)
      }).filter(_._1!=rdd._1).sortBy(-_._2).take(21).map(_._1)
      (rdd._1,recommend)
    }).collect().sortBy(_._1)
    val rddResult = spark.sparkContext.parallelize(r).toDF("id","sim")

    val resultDF = porn.join(rddResult,porn("id")===rddResult("id")).drop(rddResult("id"))
    //resultDF.write.json("porn_sim.json")
    resultDF.saveToEs("porn/recommend")
  }

  def clean(spark:SparkSession,inputPath:String,outputPath:String): Unit ={
    import spark.implicits._
    val data = spark.read.json(inputPath)
      .map(line => {
        val title         = line.getAs[String]("title")
        val categories    = line.getAs[mutable.WrappedArray[String]](1)
        val tags          = line.getAs[mutable.WrappedArray[String]](6)
        val actors        = line.getAs[mutable.WrappedArray[String]](0).map(field =>{
          field.replaceAll("[\\n\\t ]", "")
        }).filter(_.nonEmpty)
        val url           = line.getAs[String]("url")
        val image         = line.getAs[String]("image")
        val uploader_name = line.getAs[String]("uploader_name")
        val uploader_url  = line.getAs[String]("uploader_url")
        val video_url     = line.getAs[String]("video_url")
        val published_time = line.getAs[String]("published_time")
        val production    = line.getAs[mutable.WrappedArray[String]](4)
        val watch         = line.getAs[Long]("watch")
        val like          = line.getAs[Long]("like")
        (title,categories,tags,actors,url,image,uploader_name,uploader_url,video_url,published_time,production,watch,like)
      }).toDF("title","categories","tags","actors","url","image","uploader_name","uploader_url","video_url","published_time","production","watch","like")
    data.write.json(outputPath)
  }
  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
//    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum).toFloat
  }
}
