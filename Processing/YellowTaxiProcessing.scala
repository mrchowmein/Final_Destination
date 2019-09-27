import org.apache.spark.sql.types._
import java.lang.Math
import org.apache.spark.sql.DataFrame



val yellowDataPath = ("s3a://nycyellowgreentaxitrip/trip data/yellowtaxi/yellow_tripdata_2019-06.csv")
val yellowDF = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(yellowDataPath)

val dateToTimeStamp = udf((starttime: String) => { 
	starttime.split(':')(0)
})


val zipPath: String = "hdfs://ec2-35-163-178-143.us-west-2.compute.amazonaws.com:9000/zipcode_tables/taxiZoneZips.csv"

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

val taxiStartTime = yellowDF.withColumnName("tpep_pickup_datetime",dateToTimeStamp($"tpep_pickup_datetime"))