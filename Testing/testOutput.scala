import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row

//parse arguments for pw and user
val args = sc.getConf.get("spark.driver.args").split("\\s+")


//read in ref bike counts
val testRef = ("hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/testRef/2019060601RefCount.csv")
val refBikeRDD = sc.textFile(testRef)
val refBikeSplit = refBikeRDD.map(line => line.split(',')).filter(line=>line.contains("2019"))
val refBikeCount = refBikeSplit.map(line=>(line(0), line(1).toLong))

//read in processed output bike counts from db
val prop = new java.util.Properties
prop.setProperty("driver", "org.postgresql.Driver")
prop.setProperty("user", args(0))
prop.setProperty("password", args(1))

val url = "jdbc:postgresql://10.0.0.12:5432/testing"
val query = "(select * from combinedtable2 where date = '2019-06-01' and hour = '00' and bike_trip_count > 0) as combinedtable2"
val testPortion = spark.read.jdbc(url, query, prop)
val rows: RDD[Row] = testPortion.rdd
val outputBikeCount = rows.map(line=>(line(2)+""+line(3)+line(0)+line(1), line(4)+""))
val outputBikeCount2 = outputBikeCount.map(t=>(t._1, t._2.toLong))

//joined output and ref rdd
val joinedBikes = outputBikeCount2.leftOuterJoin(refBikeCount)

//compute delta between output and ref. if there is any number besides 0, then there is an incorrect calculation
val outputResults = joinedBikes.map(t=>t._1 + " " + (t._2._1 - t._2._1))
//save test log
outputResults.coalesce(1,true).saveAsTextFile("hdfs://ec2-54-68-153-54.us-west-2.compute.amazonaws.com:9000/2019060601OutputTestLog")
