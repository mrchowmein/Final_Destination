#!/bin/bash
#
#
#remove old zipcode files from hdfs
hdfs dfs -rm -r /zipcode_tables/stationZip.csv
hdfs dfs -rm -r /zipcode_tables/taxiZoneZips.csv

#run geocoding scripts to create updated zipcode list
python3 Final_Destination/TaxiZipcode/TaxiZip.py
python3 Final_Destination/CitiBikeZipcode/CitiStationZips.py

#put updated zipcode list to hdfs
hdfs dfs -put Final_Destination/TaxiZipcode/taxiZoneZips.csv /zipcode_tables
hdfs dfs -put Final_Destination/CitiBikeZipcode/stationZip.csv /zipcode_tables

#run spark script to process all datasets
spark-shell -i Final_Destination/Processing/Combined_Processing.scala --conf spark.driver.args="<userid> <password>" --jars /home/ubuntu/postgresql-42.2.8.jar
