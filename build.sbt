name := "recommendation"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "com.huaban" % "jieba-analysis" % "1.0.2"
libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.3.2"


libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "6.8.5"


