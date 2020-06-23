#!/bin/bash

JAR_PATH=./target/scala-2.11/recommendation_2.11-0.1.jar
HDFS_PATH=/user/james/

echo "Build jar"
sbt clean compile package

hdfs dfs -rm  $HDFS_PATH/recommendation_2.11-0.1.jar

echo "Upload to HDFS"
hdfs dfs -put $JAR_PATH $HDFS_PATH