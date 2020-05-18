//import com.huaban.analysis.jieba.JiebaSegmenter
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//
//object NewsCategory {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val spark = SparkSession.builder.appName("News").config("spark.master", "local").getOrCreate()
//    val filepath = "/Users/james/Desktop/News.csv"
//    val df = clean(filepath,spark)
////    df.show()
//
//    val stopWords = spark.sparkContext.textFile("/Users/james/Desktop/stopwords.txt").collect()
//    val remover = new StopWordsRemover()
//      .setStopWords(stopWords)
//      .setInputCol("token")
//      .setOutputCol("removed")
//    val removedDF = remover.transform(df)
////    removedDF.show()
//
//    val vector = new CountVectorizer()
//      .setVocabSize(15000)
//      .setInputCol("removed")
//      .setOutputCol("features")
//      .fit(removedDF)
//
//    val vectorDF = vector.transform(removedDF)
////    vectorDF.select("label","features").show(false)
//    val Array(train, predict)= vectorDF.randomSplit(Array(0.7, 0.3))
//    train.persist()
//    val lr = new LogisticRegression()
//      .setMaxIter(100)
//      .setRegParam(0.2)
//      .setElasticNetParam(0.05)
//      .setLabelCol("label")
//      .setFeaturesCol("features")
//      .fit(train)
//    train.unpersist()
//
//    val predictions = lr.transform(predict)
//    predictions.select("prediction","label","probability").show(3000,false)
//
//
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println("正確率 = " + accuracy)
//
//  }
//
//  def clean(filePath: String, spark:SparkSession): DataFrame = {
//    import spark.implicits._
//    val data = spark.read.format("csv").option("header", "true").load(filePath).flatMap(x => {
//      val title = x.getAs[String](4)
//      val t = new JiebaSegmenter().sentenceProcess(x.getAs[String](5))
//      var temp = ""
//      t.forEach(x => temp = temp + "," + x)
//      temp = temp.substring(1)
//      val token = temp.split(",")
//      val category = x.getAs[String](7)
//      var label = -1.0
//      if (category.contains("娛樂星聞")) label = 0.0
//      else if (category.contains("社會萬象")) label = 1.0
//      else if (category.contains("政治要聞")) label = 2.0
//      else label = 3.0
//      Some(label, category, title, token)
//    }).toDF("label", "category", "title","token")
//    data
//  }
//}
