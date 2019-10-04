import org.apache.spark.sql.types._
import java.lang.Math
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.catalyst.catalog.BucketSpec


val bikeDataPath = ("s3a://citibiketripdata/")
val args = sc.getConf.get("spark.driver.args").split("\\s+")


//data citi bike prep  

def loadCitiTripData (bikeDataPath : String): DataFrame =  { 

	val schema = (new StructType).add("tripduration",DoubleType,true).add("starttime",StringType,true).add("stoptime",StringType,true).add("start station id",StringType,true).add("start station name",StringType,true).add("start station latitude",StringType,true).add("start station longitude",StringType,true).add("end station id",StringType,true).add("end station name",StringType,true).add("end station latitude",StringType,true).add("end station longitude",StringType,true).add("bikeid",StringType,true).add("usertype",StringType,true).add("birth year",IntegerType,true).add("gender",StringType,true)
	val bikeData = spark.read.format("csv").option("header", "false").schema(schema).option("mode", "DROPMALFORMED").load(bikeDataPath)
	bikeData
}



val secToMinTime = udf((time_in_sec: Double) => { 
	val min = Math.round(time_in_sec/60 * 100.00)/100.00
	min
})

val dateToTimeStampBike = udf((starttime: String) => { 
	val dateHour = starttime.split(':')(0)

	if(dateHour.contains("/")){
		var oldDate = dateHour.split("[/, ]")
		if(oldDate(0).length < 2 && oldDate(0).length!=0){
			oldDate(0) = "0"+oldDate(0)
		}
		if(oldDate(1).length < 2 && oldDate(1).length!=0){
			oldDate(1) = "0"+oldDate(1)
		}
		val newDate = oldDate(2)+"-"+oldDate(0)+"-"+oldDate(1)+" "+oldDate(3)
		newDate
	} else {
		dateHour
	}
})

//geocoding

val zipPath: String = "hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/zipcode_tables/stationZip.csv"

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

val getZipWithIDTaxi = udf((startStion: String) => { 
	
	if(zipMapTaxi.contains(startStion)) 
		 if(zipMapTaxi(startStion).length >0){
		 	zipMapTaxi(startStion)
		 } else {
		 	val none = "00000"
			none
		 }
		 

	else{
		val none = "00000"
		none
	} 
	
})

val getHour = udf((starttime: String) => { 
	if(starttime.split(' ').length > 1){
		val hour = starttime.split(' ')(1)
		hour
	} else {
		val hour = "-1"
		hour
	}
	
})

val getDate = udf((starttime: String) => { 
	
	val date = starttime.split(' ')(0)

	date
})


val bikeData = loadCitiTripData(bikeDataPath)

// sc.setCheckpointDir("hdfs://ec2-35-163-178-143.us-west-2.compute.amazonaws.com:9000/checkpoint")

// bikeData.checkpoint()
val bikeDataStart = bikeData.withColumn("starttime",dateToTimeStampBike($"starttime"))
//val bikeDataDateHour = bikeDataStart.withColumn("hour",getHour($"starttime"))

val joinedDFWithZip = bikeDataStart.withColumn("start station id",getZipWithID($"start station id"))
val joinedDFWithZip1 = joinedDFWithZip.withColumn("end station id",getZipWithID($"end station id"))


// create DF for departure stations with the distribution of final destinations



def joinedDepartAndDuration = {
	
	val departureDF = joinedDFWithZip1.select("starttime", "start station id", "end station id").groupBy("starttime", "start station id", "end station id").count()
	val subCountDF = joinedDFWithZip1.select("starttime", "start station id", "end station id", "usertype").filter($"usertype" === "Subscriber").groupBy("starttime","start station id", "end station id").count().withColumnRenamed("count","sub_count")
	val joinSeqBike = Seq("starttime", "start station id", "end station id")
	val depatwithSub = departureDF.join(subCountDF, joinSeqBike)

	//calculate sub ratio and hour col
	val departSubRatioDF = depatwithSub.withColumn("sub_percent", $"sub_count" / $"count").withColumn("hour",getHour($"starttime")).withColumn("date", getDate($"starttime"))

	val durationDF = joinedDFWithZip1.select("starttime","start station id", "end station id", "tripduration").groupBy("starttime","start station id", "end station id").avg("tripduration")
	val durationInMin= durationDF.withColumn("avg(tripduration)",secToMinTime($"avg(tripduration)").alias("duration"))
	
	val deptzips_duration= departSubRatioDF.join(durationInMin, joinSeqBike)
	deptzips_duration
}


val processedBikeDF = joinedDepartAndDuration.withColumnRenamed("start station id", "start_zip").withColumnRenamed("end station id", "end_zip").withColumnRenamed("count", "bike_trip_count").withColumnRenamed("avg(tripduration)", "bike_duration").drop("starttime").drop("sub_count")
processedBikeDF.printSchema()

//println("bike count: "+processedBikeDF.count())



val yellowDataPath = ("s3a://nycyellowgreentaxitrip/trip data/yellowtaxi/")
val yellowDF = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(yellowDataPath)

val dateToTimeStamp = udf((pickupTime: String) => {
	if(pickupTime.contains(":")){
		pickupTime.split(':')(0)
	} else {
		pickupTime
	}
})

val zipPathTaxi: String = "hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/zipcode_tables/taxiZoneZips.csv"
val zipMapTaxi = createZipMap(zipPathTaxi)

val toDouble = udf((numString: String) => { 
	
	val doubleNum = numString.toDouble
	doubleNum
	
})

val taxiStartTimeYellow = yellowDF.withColumn("tpep_pickup_datetime",dateToTimeStamp($"tpep_pickup_datetime"))
val taxiWithZipsYellow = taxiStartTimeYellow.withColumn("PULocationID",getZipWithIDTaxi($"PULocationID")).withColumn("DOLocationID",getZipWithIDTaxi($"DOLocationID")).filter($"trip_distance" !== ".00").withColumn("trip_distance",toDouble($"trip_distance"))


val departureDFYellow = taxiWithZipsYellow.select("tpep_pickup_datetime", "PULocationID", "DOLocationID").groupBy("tpep_pickup_datetime", "PULocationID", "DOLocationID").count()
val creditCardCountYellow = taxiWithZipsYellow.select("tpep_pickup_datetime", "PULocationID", "DOLocationID", "payment_type").filter($"payment_type" === "1").groupBy("tpep_pickup_datetime", "PULocationID", "DOLocationID").count().withColumnRenamed("count","yellow_cc_count")
val joinSeqYellow = Seq("tpep_pickup_datetime", "PULocationID", "DOLocationID")
val departWithCCYellow = departureDFYellow.join(creditCardCountYellow, joinSeqYellow)
val departwithCCPercentYellow = departWithCCYellow.withColumn("yellow_cc_percent", $"yellow_cc_count" / $"count").withColumn("hour",getHour($"tpep_pickup_datetime")).withColumn("date", getDate($"tpep_pickup_datetime"))

val distanceDFYellow = taxiWithZipsYellow.select("tpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_distance").groupBy("tpep_pickup_datetime", "PULocationID", "DOLocationID").avg("trip_distance")
val processedyellowDF = departwithCCPercentYellow.join(distanceDFYellow, joinSeqYellow).withColumnRenamed("PULocationID", "start_zip").withColumnRenamed("DOLocationID", "end_zip").withColumnRenamed("count", "yellow_trip_count").withColumnRenamed("avg(trip_distance)", "yellow_trip_distance_avg").drop("tpep_pickup_datetime")

processedyellowDF.printSchema()



val greenDataPath = ("s3a://nycyellowgreentaxitrip/trip data/greentaxi/")
val greenDF = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED")load(greenDataPath)


val taxiStartTimeGreen = greenDF.withColumn("lpep_pickup_datetime",dateToTimeStamp($"lpep_pickup_datetime"))
val taxiWithZipsGreen = taxiStartTimeGreen.withColumn("PULocationID",getZipWithIDTaxi($"PULocationID")).withColumn("DOLocationID",getZipWithIDTaxi($"DOLocationID")).filter($"trip_distance" !== ".00").withColumn("trip_distance",toDouble($"trip_distance"))

// sc.setCheckpointDir("hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/checkpoint")
// taxiWithZips.checkpoint()

val departureDFGreen = taxiWithZipsGreen.select("lpep_pickup_datetime", "PULocationID", "DOLocationID").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").count()
// departureDF.checkpoint()

val creditCardCountGreen = taxiWithZipsGreen.select("lpep_pickup_datetime", "PULocationID", "DOLocationID", "payment_type").filter($"payment_type" === "1").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").count().withColumnRenamed("count","green_cc_count")
val joinSeqGreen = Seq("lpep_pickup_datetime", "PULocationID", "DOLocationID")
val departWithCCGreen = departureDFGreen.join(creditCardCountGreen, joinSeqGreen)
val departwithCCPercentGreen = departWithCCGreen.withColumn("green_cc_percent", $"green_cc_count" / $"count").withColumn("hour",getHour($"lpep_pickup_datetime")).withColumn("date", getDate($"lpep_pickup_datetime"))

val dispatchedCountGreen = taxiWithZipsGreen.select("lpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_type").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").count().withColumnRenamed("count","dispatch_count")
val dispatchwithDepartGreen = departureDFGreen.join(dispatchedCountGreen, joinSeqGreen)

val dispatch_percentGreen = dispatchwithDepartGreen.withColumn("green_dispatch_percent", $"dispatch_count" / $"count").drop("count")

val distanceDFGreen = taxiWithZipsGreen.select("lpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_distance").groupBy("lpep_pickup_datetime", "PULocationID", "DOLocationID").avg("trip_distance")
val processedGreenDF = departwithCCPercentGreen.join(distanceDFGreen, joinSeqGreen).join(dispatch_percentGreen, joinSeqGreen).withColumnRenamed("PULocationID", "start_zip").withColumnRenamed("DOLocationID", "end_zip").withColumnRenamed("count", "green_trip_count").withColumnRenamed("avg(trip_distance)", "green_trip_distance_avg").drop("lpep_pickup_datetime").drop("dispatch_count")
processedGreenDF.printSchema()
//println("green count: "+processedGreenDF.count())



val joinSeqComb = Seq("date", "hour", "start_zip", "end_zip")
//val combinedDFs = procssedyellowDF.join(processedBikeDF, joinSeqComb).join(processedGreenDF, joinSeqComb)

val combinedDFs = processedBikeDF.join(processedyellowDF, joinSeqComb, "full").join(processedGreenDF, joinSeqComb, "full")

val prop = new java.util.Properties
prop.setProperty("driver", "org.postgresql.Driver")
prop.setProperty("user", args(0))
prop.setProperty("password", args(1))

val url = "jdbc:postgresql://10.0.0.12:5432/testing"
val table = "combined_multimodal_table"

combinedDFs.write.mode("Overwrite").jdbc(url, table, prop)









