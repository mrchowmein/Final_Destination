val zipPath: String = "hdfs://ec2-35-163-178-143.us-west-2.compute.amazonaws.com:9000/zipcode_tables/"
val zipTable = sc.textFile(zipPath)
val zipRDD = zipTable.map(line => line.split(','))
val idZipRDD = zipRDD.map(line=>(line(0),line(1)))

val bikeRDD = sc.textFile("s3a://citibiketripdata/201907-citibike-tripdata.csv")
//val bikeRDD = sc.textFile("s3a://citibiketripdata/")
val bikeIdStatRDD = bikeRDD.map(line => line.split(',')).map(line=>(line(3),1))
val reducedTrips = bikeIdStatRDD.reduceByKey(_+_)

val joinedZipTrips = idZipRDD.join(reducedTrips)

val tripZipRDD = joinedZipTrips.map(t => (t._2._2,t._2._1)).sortByKey(true)





//read citibike data as df


//val bikeRDD = sc.textFile("s3a://citibiketripdata/201907-citibike-tripdata.csv")




import org.apache.spark.sql.types._
import java.lang.Math


val bikeDataPath = ("s3a://citibiketripdata/201907-citibike-tripdata.csv")


/*
|-- tripduration: string (nullable = true)
 |-- starttime: string (nullable = true)
 |-- stoptime: string (nullable = true)
 |-- start station id: string (nullable = true)
 |-- start station name: string (nullable = true)
 |-- start station latitude: string (nullable = true)
 |-- start station longitude: string (nullable = true)
 |-- end station id: string (nullable = true)
 |-- end station name: string (nullable = true)
 |-- end station latitude: string (nullable = true)
 |-- end station longitude: string (nullable = true)
 |-- bikeid: string (nullable = true)
 |-- usertype: string (nullable = true)
 |-- birth year: string (nullable = true)
 |-- gender: string (nullable = true)



*/

// def lowerRemoveAllWhitespace(schema: struct) = {
//   val bikeData = spark.read.format("csv").option("header", "false").schema(schema).option("mode", "DROPMALFORMED").load(bikeDataPath)
//   val bikeDF2 = bikeData.withColumn("starttime",dateToTimeStamp($"starttime"))
//   val bikeDF3 = bikeDF2.withColumn("stoptime",dateToTimeStamp($"stoptime"))
  
// }

//data citi bike prep  

def loadCitiTripData (bikeDataPath : String): DataFrame =  { 

	val schema = new StructType().add("tripduration",DoubleType,true).add("starttime",StringType,true).add("stoptime",StringType,true).add("start station id",StringType,true).add("start station name",StringType,true).add("start station latitude",StringType,true).add("start station longitude",StringType,true).add("end station id",StringType,true).add("end station name",StringType,true).add("end station latitude",StringType,true).add("end station longitude",StringType,true).add("bikeid",StringType,true).add("usertype",StringType,true).add("birth year",IntegerType,true).add("gender",StringType,true)
	val bikeData = spark.read.format("csv").option("header", "false").schema(schema).option("mode", "DROPMALFORMED").load(bikeDataPath)
	bikeData
}



val secToMinTime = udf((time_in_sec: Double) => { 
	val min = Math.round(time_in_sec/60 * 100.00)/100.00
	min
})

val dateToTimeStamp = udf((starttime: String) => { 
	starttime.split(':')(0)
})


val bikeData = loadCitiTripData(bikeDataPath)
val bikeDF2 = bikeData.withColumn("starttime",dateToTimeStamp($"starttime"))
val bikeDF3 = bikeDF2.withColumn("stoptime",dateToTimeStamp($"stoptime"))

// create DF for departure stations with the distribution of final destinations

def joinedDepartAndDuration = {

	val departureDF = bikeDF3.select("starttime", "start station id", "end station id").groupBy("starttime", "start station id", "end station id").count()

	val durationDF = bikeDF3.select("starttime", "start station id", "end station id", "tripduration").groupBy("starttime", "start station id", "end station id").avg("tripduration")
	val durationInMin= durationDF.withColumn("avg(tripduration)",secToMinTime($"avg(tripduration)").alias("duration"))
	val joinSeq = Seq("starttime", "start station id", "end station id")
	val deptzips_duration= departureDF.join(durationInMin, joinSeq).orderBy($"starttime".asc, $"start station id".asc)
	deptzips_duration
}


val joinedDF = joinedDepartAndDuration

//departureDF.orderBy($"starttime".asc, $"start station id".asc).show()

// // create DF for arrival stations with the distribution of start destinations
// val arrivalDF = bikeDF3.select("starttime", "end station id", "start station id").groupBy("starttime", "end station id", "start station id").count()
// arrivalDF.orderBy($"starttime".asc, $"end station id".asc).show()



// create DF for depart stations with the distribution of age
// val ageDF_depart = bikeDF3.select("starttime", "start station id", "end station id", "birth year").groupBy("starttime", "start station id", "end station id").avg("birth year")

// //ageDF.orderBy($"starttime".asc, $"start station id".asc).show()

// // create DF for arrival stations with the distribution of age
// val ageDF_arrival = bikeDF3.select("starttime", "end station id", "start station id", "birth year").groupBy("starttime", "end station id", "start station id").avg("birth year")
// ageDF_arrival.orderBy($"starttime".asc, $"end station id".asc).show()

// val yearToAge = udf((yearInt: Integer) => { 
// 	val age = 2019-yearInt
// 	age
// })

//calcuated age df
// val ageDF_arrival2 = ageDF_arrival.withColumn("avg(birth year)",yearToAge($"avg(birth year)").alias("age"))

// val ageDF_depart2 = ageDF_depart.withColumn("avg(birth year)",yearToAge($"avg(birth year)").alias("age"))
// ageDF_depart2.orderBy($"starttime".asc, $"start station id".asc).show()


//duration dataframe








//joined age, duration and endstation count




