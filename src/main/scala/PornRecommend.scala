import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{ Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable

object PornRecommend {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").appName("MovieRecommend").getOrCreate()
    import spark.implicits._
    val title = "GERMAN SCOUT - BIG HANGING TITS TEEN ANISSA CHEAT BF AT OUTDOOR CASTING SEX"
    val tags = "big cock,big boobs,teenager,young,public,german scout,public agent,teen,hanging tits,saggy tits,big tits,money,cash,anissa jolie,big tits teen,outdoor".split(",")
    val df = Seq((title,tags)).toDF("title","tags")
    val modelPath = "./PornTagsWord2Vec.model"
    val testData = Word2VecModel.load(modelPath).transform(df).select("features")

    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrUdf = udf(toArr)
    val dataWithFeaturesArr = testData.withColumn("features_arr",toArrUdf('features)).drop("tags","features")
        .map(dataset => {
          val array = dataset.getAs[mutable.WrappedArray[Double]](0).toArray
          array
        }).first()

    val metadata = spark.read.json("/Users/james/Desktop/porn.json")
      .filter("tags is not null")
      .map(x=>{
        val actors = x.getAs[mutable.WrappedArray[String]](0)
        val features = x.getStruct(1).getAs[mutable.WrappedArray[Double]](3).toArray
        val id = x.getLong(2)
        val image = x.getString(3)
        val like = x.getLong(4)
        val published_time = x.getString(5)
        val tags = x.getAs[mutable.WrappedArray[String]](6)
        val title = x.getString(7)
        val url = x.getString(8)
        val watch = x.getLong(9)
        val sim = cosineSimilarity(features,dataWithFeaturesArr)
        (id,title,actors,tags,image,url,published_time,like,watch,features,sim)
    }).toDF("id","title","actors","tags","image","url","published_time","like","watch","features","sim").filter($"title"=!=title && !$"sim".isNaN )
      .select("id","title","tags","sim").orderBy(desc("sim")).show(false)


  }
  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
//    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }
}
