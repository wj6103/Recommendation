import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, IDF, Word2Vec}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}

object ContentBasedFiltering {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val apps = spark.read.option("header",true)
      .csv("/Users/james/Desktop/ml-latest-small/tags.csv")
      .drop("userId").drop("timestamp").toDF()
    apps.registerTempTable("apps")
    val data = apps.sqlContext.sql("select movieId,concat_ws(',', collect_list(tag)) from apps group by movieId")
      .filter(x => {x.getString(1).split(",").length < 10})
      .map((x => (x.getString(0),Seq(x.getString(1)),Seq(x.getString(1))))).toDF("id","keywords","keyword2")

    val word2Vec = new Word2Vec()
      .setInputCol("keywords")
      .setOutputCol("features")
      .setVectorSize(100)
      .setMaxIter(5)
      .setMinCount(1)
      .fit(data)
//    val w2v = word2Vec.transform(data)
//    w2v.show(false)
    val vector1 = word2Vec.getVectors.select(col("word").as("itemA"),col("vector").as("vectorA"))
    val vector2 = word2Vec.getVectors.select(col("word").as("itemB"),col("vector").as("vectorB"))

    val crossdata = vector1.crossJoin(vector2)

    crossdata.show(false)

//    val brp = new BucketedRandomProjectionLSH()
//      .setBucketLength(4.0)
//      .setNumHashTables(10)
//      .setInputCol("features")
//      .setOutputCol("hashes")
//    val brpModel = brp.fit(w2v)
//    val tsDf = brpModel.transform(w2v)
//    tsDf.show(false)

//    val brpDf = brpModel.approxSimilarityJoin(tsDf, tsDf, 0.015, "EuclideanDistance")
////    brpDf.show(false)
//
//    val getIdFun = udf((input:Row)=> {
//      input(0).toString.toInt;
//    });
//    val corrDf = brpDf.withColumn("id",getIdFun(col("datasetA")))
//      .withColumn("id_sim",getIdFun(col("datasetB")))
//      .drop("datasetA").drop("datasetB").drop("EuclideanDistance")
////    corrDf.show(false)
//
//    corrDf.createOrReplaceTempView("test")
//    val resDf = spark.sql("select id,concat_ws(',',collect_set(id_sim)) as sim from test where id != id_sim group by id")
//    resDf.show(false)
  }
}
