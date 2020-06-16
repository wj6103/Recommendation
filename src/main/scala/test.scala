import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Word2Vec}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._

object test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("MovieRecommend")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max","512")
      .config("spark.memory.useLegacyMode","true")
      .config("spark.storage.memoryFraction", "0.6")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "300s")
      .config("es.index.auto.create", "true")
      .config("es.nodes.wan.only", "false")
      .config("es.nodes", "10.205.48.52")
      .config("es.port", "9200")
      .getOrCreate()

    import spark.implicits._
    val hdfsPath = "/user/james/pornhub.json"
    val filPath = "file:///Users/james/Desktop/test.json"
    val porn = spark.read.json(hdfsPath)
      .filter(size($"tags") > 0)
      .withColumn("id",monotonically_increasing_id)
      .drop("production").drop("uploader_name").drop("uploader_url").drop("url")
      .toDF()



    val w2v = new Word2Vec()
      .setInputCol("tags")
      .setOutputCol("features")
      .setMaxIter(5)
      .setVectorSize(300)
      .setMinCount(1)
      .fit(porn)
    val w2vResult = w2v.transform(porn)

    val myDataframe = w2vResult.select("id", "features")
      .map(x => (x.getLong(0), org.apache.spark.mllib.linalg.Vectors.fromML(x.getAs[DenseVector](1))))

    val rdd = myDataframe.as[(Long, org.apache.spark.mllib.linalg.Vector)].rdd.zipWithIndex()
//      .repartition(args(0).toInt)
//      .repartition(6)

    val indexMap = rdd.map { case ((id, vec), index) => (index, id) }.collectAsMap()
    val irm = new IndexedRowMatrix(rdd.map { case ((id, vec), index) => IndexedRow(index, vec) })
    val cm = irm.toCoordinateMatrix()
    val cmT = cm.transpose()
    val rm = cmT.toRowMatrix()
    val cosSim = rm.columnSimilarities()
    cosSim.entries.toDF().orderBy("i","j").show(100)

    val r1 = cosSim.entries.map(e => (indexMap(e.i), indexMap(e.j), e.value)).toDF("id1","id2","sim")
    val r2 = cosSim.entries.map(e => (indexMap(e.j), indexMap(e.i), e.value)).toDF("id1","id2","sim")
    val f = r1.union(r2).orderBy(asc("id1"),desc("sim"))
      .map(x=>{
        (x.getLong(0),x.getLong(1).toString+"-"+x.getDouble(2).toString)
      }).groupBy("_1").agg(concat_ws(",",collect_list("_2")) as "sim")
        .map(x=>{(x.getLong(0),x.getString(1).split(",").slice(0,20))}).toDF("id","sim")
//    f.show(false)
    val result = porn.join(f,f("id")===porn("id")).drop(f("id"))
//    result.show(false)
    result.saveToEs("porn/recommend",Map("es.mapping.id" -> "id"))
  }
}


