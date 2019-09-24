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

val dateToTimeStamp = udf((starttime: String) => { 
	starttime.split(':')(0)
})



//read citibike data as df
//val bikeDataPath = ("s3a://citibiketripdata/201907-citibike-tripdata.csv")

val bikeRDD = sc.textFile("s3a://citibiketripdata/201907-citibike-tripdata.csv")

val bikeNoHead = bikeRDD.map(line => line.split(",")).filter(line => line(0).forall(_.isDigit))



//import org.apache.spark.sql.types._

val schema = new StructType().add("tripduration",StringType,true).add("starttime",StringType,true).add("stoptime",StringType,true).add("start station id",StringType,true).add("start station name",StringType,true).add("start station latitude",StringType,true).add("start station longitude",StringType,true).add("end station id",StringType,true).add("end station name",StringType,true).add("end station latitude",StringType,true).add("end station longitude",StringType,true).add("bikeid",StringType,true).add("usertype",StringType,true).add("birth year",StringType,true).add("gender",StringType,true)


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


val bikeData = spark.read.format("csv").option("header", "false").schema(schema).option("mode", "DROPMALFORMED").load(bikeDataPath)
//val bikeDF = spark.read.format("csv").option("header", "false").option("mode", "DROPMALFORMED").load(bikeDataPath)

val bikeDF  = bikeData.filter(row => row.getAs[String]("tripduration").matches("""\d+"""))


val bikeDF2 = bikeDF.withColumn("starttime",dateToTimeStamp($"starttime"))

val bikeDF3 = bikeDF2.withColumn("stoptime",dateToTimeStamp($"stoptime"))

bikeDF3.select("starttime", "start station id", "stoptime", "end station id").show()

val bikeDF4 = bikeDF3.select("starttime", "start station id", "stoptime", "end station id").groupBy("starttime", "start station id", "end station id").count()

bikeDF4.orderBy($"starttime".asc, $"start station id".asc).show()