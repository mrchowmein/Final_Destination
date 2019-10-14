
#!/bin/bash
#
#
#remove old files from hdfs
hdfs dfs -rm -r /2019060601OutputTestLog
hdfs dfs -rm -r /testRef

#create refcount file from sample
python3 ./Testing/createRefCount.py
hdfs dfs -mkdir /testRef
#move refcount file to hdfs
hdfs dfs -put 2019060601RefCount.csv /testRef

#run output test
spark-shell -i testOutput.scala --conf spark.driver.args="<user pw>" --jars /home/ubuntu/postgresql-42.2.8.jar
