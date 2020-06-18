
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import org.slf4j
import org.slf4j.LoggerFactory
import scalaj.http.Http


object MovieRecommendContentBasedFiltering {
  def main(args: Array[String]): Unit = {
    var startTime = System.nanoTime
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("MovieRecommend")
//      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "300s")
      .config("es.index.auto.create", "true")
      .config("es.nodes.wan.only", "false")
      .config("es.nodes", "10.205.48.52")
      .config("es.port", "9200")
      .getOrCreate()

    import spark.implicits._
    val hdfsPath = "/datalake/data/video"
    val porn = spark.read.parquet(hdfsPath)
      .filter(size($"tags") > 0)
      .withColumn("id", $"item_id")
      .drop($"item_id")
      .drop($"spider_name")
      .drop($"created_time")
      .drop($"save_date")
      //          .limit(4)
      .toDF()

    val w2v = new Word2Vec()
      .setInputCol("tags")
      .setOutputCol("features")
      .setMaxIter(5)
      .setVectorSize(300)
      .setMinCount(1)
      .fit(porn)
    val w2vResult = w2v.transform(porn)


    //== v2 ==
    val compare = w2vResult.select("id", "features").map(a => (a.getString(0), a.getAs[DenseVector](1).toArray)).rdd.zipWithIndex().cache()
    val indexMap = compare.map { case ((id, vec), index) => (index, id) }.collectAsMap()
    val comArray = compare.collect()
    val comRdd = compare.repartition(args(0).toInt)
    //        val comRdd = compare.repartition(6)
    val br_c = spark.sparkContext.broadcast(comArray)

    val r = comRdd.mapPartitions(it => {
      val c = br_c.value
      it.flatMap(rdd => {
        val recommend = c.flatMap(d => {
          if (rdd._2 < d._2) {
            val sim = cosineSimilarity(rdd._1._2, d._1._2).formatted("%.4f").toDouble
            Array((rdd._2, d._2, sim), (d._2, rdd._2, sim))
          }
          else
            Array.empty[(Long, Long, Double)]
        })
        recommend
      })
    })
    val df = r
      .groupBy(_._1)
      .mapValues(_.toList.sortBy(-_._3).take(20).map(
        x =>
          indexMap(x._2) + "-" + x._3
      )).map(x => (indexMap(x._1), x._2)).toDF("_1", "sim")
    val result = porn.join(df, df("_1") === porn("id")).drop(df("_1")).repartition(6)
    result.show(false)
    result.saveToEs("porn/recommend", Map("es.mapping.id" -> "id"))
    //== v2 ==

    //== v1 ==
    //        val compare = w2vResult.select("id", "features").map(a => (a.getString(0), a.getAs[DenseVector](1).toArray)).rdd.cache()
    //            val comArray = compare.collect()
    //            val comRdd = compare.repartition(args(0).toInt)
    //    //            val comRdd = compare.repartition(6)
    //            val br_c = spark.sparkContext.broadcast(comArray)
    //            val r = comRdd.map(rdd => {
    //              val recommend = br_c.value.map(d => {
    //                val sim = cosineSimilarity(rdd._2, d._2)
    //                (d._1, sim)
    //              }).filter(_._1 != rdd._1).sortBy(-_._2).take(21).map(_._1)
    //              (rdd._1, recommend)
    //            }).collect().sortBy(_._1)
    //            val rddResult = spark.sparkContext.parallelize(r).toDF("id", "sim")
    //
    //            val resultDF = porn.join(rddResult, porn("id") === rddResult("id")).drop(rddResult("id"))
    //    //        resultDF.show()
    //            resultDF.saveToEs("porn_t/recommend", Map("es.mapping.id" -> "id"))
    //== v1 //


    //send line message
    val endTime = TimeUnit.MINUTES.convert(System.nanoTime - startTime ,TimeUnit.NANOSECONDS)
    val fs = FileSystem.get(new Configuration()).listStatus(new Path("/datalake/checkpoint/parquet/video/offsets"))
    val offset = fs.indexOf(fs.last)

    val today = spark.read.json("/datalake/checkpoint/parquet/video/offsets/" + offset)
      .select("video").where("video is not null").collect()(0)(0).toString
    val yestoday = spark.read.json("/datalake/checkpoint/parquet/video/offsets/" + (offset - 1))
      .select("video").where("video is not null").collect()(0)(0).toString
    val updateFilmNum = today.substring(1, today.length - 1).toInt - yestoday.substring(1, yestoday.length - 1).toInt

    val message =
      s"""{"messages":[{"type": "text","text": "今天的更新已經完成"},
         |{"type": "text","text": "新增影片數量 : $updateFilmNum"},
         |{"type": "text","text": "更新時間為 $endTime 分鐘"}]}""".stripMargin
    val sendLineMessage = Http("https://api.line.me/v2/bot/message/broadcast")
      .postData(message)
      .header("content-type", "application/json")
      .header("Authorization", "Bearer {fh/W8kiTXOCvTwk7tO62cd4OhrB5KAX3KlcyQUAexprLstLiwd9r54vIRirC8/83SoQIFdBUoEId/N78uIgOAkod1z4Tv/IUoZlGEy7II93KwOePYx4V0cmiRxnoS3U6IHYS1bv1G2Q1g7uqlc63vwdB04t89/1O/w1cDnyilFU=}")
      .asString
  }

  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    //    require(x.size == y.size)
    dotProduct(x, y) / (magnitude(x) * magnitude(y))
  }

  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for ((a, b) <- x zip y) yield a * b) sum
  }

  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map (i => i * i) sum).toFloat
  }

}
