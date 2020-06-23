#!/bin/bash

echo "-----------Start-----------"

echo "Submit Video"
curl --location --request POST 'http://ai-gateway-01.silkrode.com.tw:8998/batches' \
  --header 'Content-Type: application/json' \
  --data-raw '{
	"file": "/user/james/recommendation_2.11-0.1.jar",
	"proxyUser": "james",
	"className": "MovieRecommendContentBasedFiltering",
	"args": ["700"],
	"driverMemory": "4G",
	"numExecutors": 40,
	"executorCores": 5,
	"executorMemory": "12G",
	"conf": {"spark.jars.packages": "org.elasticsearch:elasticsearch-hadoop:7.5.1,commons-httpclient:commons-httpclient:3.1,org.scalaj:scalaj-http_2.11:2.4.2"}
}'

echo ""
echo "-----------End-----------"
