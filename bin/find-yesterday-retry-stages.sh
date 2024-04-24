#!/bin/bash
minEndDate=`date -d "-2 day" +"%Y-%m-%d"`T00:00:00Z
maxEndDate=`date -d "-1 day" +"%Y-%m-%d"`T00:00:00Z
spark-submit --master local\[1\] \
  --class com.baidu.spark.history.FindRetryStages \
  ./spark-utils-1.0-SNAPSHOT.jar \
  --conf spark.history.server.address=http://bmr-master-4096a55-1:8701 \
  --conf spark.app.check.min.duration.ms=600000 \
  --conf minEndDate=${minEndDate} \
  --conf maxEndDate=${maxEndDate}
  