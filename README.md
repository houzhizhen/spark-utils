# spark-utils

## 1. GetDriverClassPath
查看 Driver 的 CLASSPATH
```bash
/opt/bmr/spark-3.2.0/bin/spark-submit  \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 1 \
 --executor-cores 1 \
 --class org.houzhizhen.zlass.GetDriverClassPath \
  ./spark-utils-1.0-SNAPSHOT.jar
```
打开 Driver 的 stdout，里面打印了 `getJavaClassPath:`，包含了 classpath.

## 2. GetExecutorClassPath
查看 Executor 的 CLASSPATH.
```bash
/opt/bmr/spark-3.2.0/bin/spark-submit  \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 1 \
 --executor-cores 1 \
 --class org.houzhizhen.zlass.GetExecutorClassPath  ./spark-utils-1.0-SNAPSHOT.jar
```
打开 Executor 的 stdout，里面打印了 `getJavaClassPath:`，包含了 classpath.

## 3. GetExecutorClassPath
把 classpath 下的所有目录和文件，上传至分布式文件系统。接收一个参数，指定上传到分布式文件系统的目录。
```bash
/opt/bmr/spark-3.2.0/bin/spark-submit  \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 1 \
 --executor-cores 1 \
 --conf spark.yarn.maxAppAttempts=1  \
 --class org.houzhizhen.zlass.UploadFilesDriverClasspath  \
 ./spark-utils-1.0-SNAPSHOT.jar hdfs://bmr-master-7ec6049:8020/home/spark/houzhizhen/spark-jars1
```
打开 Driver 的 stdout，里面打印了上传的日志.

## 4. ESSTest

```bash
spark-submit  \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 700 \
 --executor-cores 1 \
 --conf spark.yarn.maxAppAttempts=1  \
 --conf spark.driver.memory=8G \
 --conf spark.shuffle.compress=false \
 --conf spark.driver.memory=8G \
 --class com.baidu.spark.utils.shuffle.ESSTest  \
 ./spark-utils-1.0-SNAPSHOT.jar \
 7 104857600 10000
```
ESSTest 有3个参数：
第 1 个参数是 mapPartitions, 执行 ShuffleMapTask 的 数量.
第 2 个参数是 outputCountPerMapPartition, 每个 Map Task 输出的行数据的个数。
第 3 个参数是 reducePartitions, 按照 key 进行 repartition 的个数。

key 的类型是 long，value 的类型是长度为 1024 的随机数组。

## 5. FindRetryStages（查找重试的 spark stages）
```bash
spark-submit --master local\[1\] \
  --class com.baidu.spark.history.FindRetryStages \
  target/spark-utils-1.0-SNAPSHOT.jar \
  --conf spark.history.server.address=http://localhost:8701 \
  --conf spark.app.check.min.duration.ms=10 \
  --conf minEndDate=2024-04-18T00:00:00Z \
  --conf maxEndDate=2024-04-19T00:00:00Z
```

find-yesterday-retry-stages.sh 
```bash
#!/bin/bash
export MONITOR_LOG_DIR=${MONITOR_LOG_DIR:-"/mnt/bmr/log/spark3/monitor"}
export LOGFILE=${MONITOR_LOG_DIR}/`date +"%Y%m%d-%H%M%S"`-hive-metastore-monitor.log

echo MONITOR_LOG_DIR=${MONITOR_LOG_DIR} >> ${LOGFILE}
if [ ! -d ${MONITOR_LOG_DIR} ]; then
  mkdir -p ${MONITOR_LOG_DIR}
fi

## delete obsolete logs
DELETE_LOGFILE=`date -d "-30 day" +"%Y%m%d"`
rm -rf ${MONITOR_LOG_DIR}/${DELETE_LOGFILE}*

minEndDate=`date -d "-2 day" +"%Y-%m-%d"`T00:00:00Z
maxEndDate=`date -d "-1 day" +"%Y-%m-%d"`T00:00:00Z
spark-submit --master local\[1\] \
  --class com.baidu.spark.history.FindRetryStages \
  ./spark-utils-1.0-SNAPSHOT.jar \
  --conf spark.history.server.address=http://bmr-master-4096a55-1:8701 \
  --conf spark.app.check.min.duration.ms=600000 \
  --conf minEndDate=${minEndDate} \
  --conf maxEndDate=${maxEndDate} >> ${LOGFILE} 2>$1 

``` 
crontab 
```bash
30 6 * * * timeout 2h sh /xxx/find-yesterday-retry-stages.sh
```