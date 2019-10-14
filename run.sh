#!/bin/bash
#
#
#remove old zipcode files from hdfs
hdfs dfs -rm -r /zipcode_tables/stationZip.csv
hdfs dfs -rm -r /zipcode_tables/taxiZoneZips.csv

#run geocoding scripts to create updated zipcode list
python3 ./TaxiZipcode/TaxiZip.py
python3 ./CitiBikeZipcode/CitiStationZips.py

#put updated zipcode list to hdfs
hdfs dfs -put ./TaxiZipcode/taxiZoneZips.csv /zipcode_tables
hdfs dfs -put ./CitiBikeZipcode/stationZip.csv /zipcode_tables

#run spark script to process all datasets
spark-shell -i ./Processing/Combined_Processing.scala --conf spark.driver.args="$1 $2" --jars /home/ubuntu/postgresql-42.2.8.jar
