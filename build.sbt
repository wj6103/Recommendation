lazy val root = (project in file(".")).
  settings(
    name := "recommendation",
    version := "0.1",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("MovieRecommendContentBasedFiltering")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "7.5.1"
libraryDependencies += "redis.clients" % "jedis" % "3.3.0"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"


