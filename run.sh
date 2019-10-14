#!/bin/bash
#
#
# to update zips, run manually
#run spark script to process all datasets
spark-shell -i ./Processing/Combined_Processing.scala --conf spark.driver.args="$1 $2" --jars /home/ubuntu/postgresql-42.2.8.jar
