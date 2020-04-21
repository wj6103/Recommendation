import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

import scala.collection.mutable

object MovieRecommendContentBasedFiltering {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").appName("MovieRecommend").getOrCreate()
    //clean(spark,"/Users/james/Desktop/pornhub_crawler.jl","/Users/james/Desktop/pornhub.json")
    val porn = spark.read.json("/Users/james/Desktop/pornhub.json").withColumn("id",monotonically_increasing_id)
      .select("id","title","actors","tags","watch","like","image","video_url","published_time").filter("tags is not null").toDF()

    val w2v = new Word2Vec()
      .setInputCol("tags")
      .setOutputCol("features")
      .setMaxIter(5)
      .setVectorSize(300)
      .setMinCount(1)
      .fit(porn)
    val output = w2v.transform(porn)
    output.show()
    output.write.json("/Users/james/Desktop/porn.json")
    w2v.save("/Users/james/Desktop/PornTagsWord2Vec.model")


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
}
