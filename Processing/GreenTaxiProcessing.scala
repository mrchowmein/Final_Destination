import org.apache.spark.sql.types._
import java.lang.Math
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.catalog.BucketSpec


val greenDataPath = ("s3a://nycyellowgreentaxitrip/trip data/greentaxi/")

val args = sc.getConf.get("spark.driver.args").split("\\s+")
/*

root
 |-- VendorID: string (nullable = true)
 |-- tpep_pickup_datetime: string (nullable = true)
 |-- tpep_dropoff_datetime: string (nullable = true)
 |-- passenger_count: string (nullable = true)
 |-- trip_distance: string (nullable = true)
 |-- RatecodeID: string (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: string (nullable = true)
 |-- DOLocationID: string (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: string (nullable = true)
 |-- extra: string (nullable = true)
 |-- mta_tax: string (nullable = true)
 |-- tip_amount: string (nullable = true)
 |-- tolls_amount: string (nullable = true)
 |-- improvement_surcharge: string (nullable = true)
 |-- total_amount: string (nullable = true)
 |-- congestion_surcharge: string (nullable = true)

 root
 |-- VendorID: string (nullable = true)
 |-- tpep_pickup_datetime: string (nullable = true)
 |-- tpep_dropoff_datetime: string (nullable = true)
 |-- passenger_count: string (nullable = true)
 |-- trip_distance: string (nullable = true)
 |-- pickup_longitude: string (nullable = true)
 |-- pickup_latitude: string (nullable = true)
 |-- RatecodeID: string (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- dropoff_longitude: string (nullable = true)
 |-- dropoff_latitude: string (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: string (nullable = true)
 |-- extra: string (nullable = true)
 |-- mta_tax: string (nullable = true)
 |-- tip_amount: string (nullable = true)
 |-- tolls_amount: string (nullable = true)
 |-- improvement_surcharge: string (nullable = true)
 |-- total_amount: string (nullable = true)

*/


val greenDF = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED")load(greenDataPath)

val dateToTimeStamp = udf((starttime: String) => { 
	starttime.split(':')(0)
})


val zipPath: String = "hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/zipcode_tables/taxiZoneZips.csv"

def createZipMap (zipTablePath : String) = {
	val zipTable = sc.textFile(zipTablePath)
	val zipRDD = zipTable.map(line => line.split(','))
	val idZipRDD = zipRDD.map(line=>(line(0),line(1))).collectAsMap()
	idZipRDD

}


val zipMap = createZipMap(zipPath)


val getZipWithID = udf((startStion: String) => { 
	
	if(zipMap.contains(startStion)) 
		 if(zipMap(startStion).length >0){
		 	zipMap(startStion)
		 } else {
		 	val none = "00000"
			none
		 }
		 

	else{
		val none = "00000"
		none
	} 
	
})

val toDouble = udf((numString: String) => { 
	
	val doubleNum = numString.toDouble
	doubleNum
	
})

val getHour = udf((starttime: String) => { 
	val hour = starttime.split(' ')(1)

	hour
})

val getDate = udf((starttime: String) => { 
	val date = starttime.split(' ')(0)

	date
})

val taxiStartTime = greenDF.withColumn("lpep_pickup_datetime",dateToTimeStamp($"lpep_pickup_datetime"))
val taxiWithZips = taxiStartTime.withColumn("PULocationID",getZipWithID($"PULocationID")).withColumn("DOLocationID",getZipWithID($"DOLocationID")).filter($"trip_distance" !== ".00").withColumn("trip_distance",toDouble($"trip_distance"))

// sc.setCheckpointDir("hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/checkpoint")
// taxiWithZips.checkpoint()

val departureDF = taxiWithZips.select("lpep_pickup_datetime", "PULocationID", "DOLocationID").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").count()
// departureDF.checkpoint()

val creditCardCount = taxiWithZips.select("lpep_pickup_datetime", "PULocationID", "DOLocationID", "payment_type").filter($"payment_type" === "1").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").count().withColumnRenamed("count","cc_count")
val joinSeq = Seq("lpep_pickup_datetime", "PULocationID", "DOLocationID")
val departWithCC = departureDF.join(creditCardCount, joinSeq)
val departwithCCPercent = departWithCC.withColumn("cc_percent", $"cc_count" / $"count").withColumn("hour",getHour($"lpep_pickup_datetime")).withColumn("date", getDate($"lpep_pickup_datetime"))

val dispatchedCount = taxiWithZips.select("lpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_type").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").count().withColumnRenamed("count","dispatch_count")
val dispatchwithDepart = departureDF.join(dispatchedCount, joinSeq)

val dispatch_percent = dispatchwithDepart.withColumn("dispatch_percent", $"dispatch_count" / $"count").drop("count")

val distanceDF = taxiWithZips.select("lpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_distance").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").avg("trip_distance")
val departCCDistDF= departwithCCPercent.join(distanceDF, joinSeq).join(dispatch_percent, joinSeq).withColumnRenamed("cc_percent", "green_cc_percent").drop("cc_count").withColumnRenamed("avg(trip_distance)", "green_avg_dist").withColumnRenamed("count", "green_count")
//departCCDistDF.collect()

//:require postgresql-42.2.8.jar
val prop = new java.util.Properties
prop.setProperty("driver", "org.postgresql.Driver")
prop.setProperty("user", args(0))
prop.setProperty("password", args(1))

val url = "jdbc:postgresql://10.0.0.12:5432/testing"
val table = "green_taxi_table2"


departCCDistDF.write.mode("Overwrite").jdbc(url, table, prop)
