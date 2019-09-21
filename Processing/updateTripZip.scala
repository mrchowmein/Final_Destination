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