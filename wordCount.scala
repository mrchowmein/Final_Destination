import java.util.Calendar

val file = sc.textFile("hdfs://ec2-34-210-161-85.us-west-2.compute.amazonaws.com:9000/user/alice.txt")

val counts = file.flatMap(line => line.split(" ")).map(word=> (word,1)).reduceByKey(_+_)

val now = Calendar.getInstance()
val currentMinute = now.get(Calendar.MINUTE)

val currentHour = now.get(Calendar.HOUR_OF_DAY)

counts.coalesce(1,true).saveAsTextFile("wordCount"+currentHour+currentMinute)

System.exit(0)
