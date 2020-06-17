
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import org.slf4j
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.row_number


object MovieRecommendContentBasedFiltering {
  private val logger: slf4j.Logger = LoggerFactory.getLogger(MovieRecommendContentBasedFiltering.getClass)

  def main(args: Array[String]): Unit = {
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
//      .limit(4)
      .toDF()

    val w2v = new Word2Vec()
      .setInputCol("tags")
      .setOutputCol("features")
      .setMaxIter(5)
      .setVectorSize(300)
      .setMinCount(1)
      .fit(porn)
    val w2vResult = w2v.transform(porn)


    //===================
    val compare = w2vResult.select("id", "features").map(a => (a.getString(0), a.getAs[DenseVector](1).toArray)).rdd.zipWithIndex().cache()
    val indexMap = compare.map { case ((id, vec), index) => (index, id) }.collectAsMap()
    val comArray = compare.collect()
        val comRdd = compare.repartition(args(0).toInt)
//    val comRdd = compare.repartition(6)
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
//    result.show(false)
    result.saveToEs("porn/recommend", Map("es.mapping.id" -> "id"))
    //==================


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
