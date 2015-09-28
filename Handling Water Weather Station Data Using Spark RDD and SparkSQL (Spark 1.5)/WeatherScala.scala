	import org.apache.spark.rdd.RDD
	import java.text.SimpleDateFormat
	import java.util.{Calendar, Date}
	import scala.util.Random

	    val inputFile = "/home/user/MySpark/Weather Spark/data.txt"

	    val input = sc.textFile(inputFile)  


// Define the wind schema using a case class.
case class Wind(STAID: String, SOUID: String, DATE:String, DD:Int, Q_DD:Int)



//eliminer les header des fichiers de la weather database
val in = input.mapPartitionsWithIndex((partitionIdx: Int, lines: Iterator[String]) => {
  if (partitionIdx == 0) {
    lines.drop(21)
  }
  lines  
})

	/*pr tester que le header est bien eliminé*/
	/*in.take(4).foreach(x => println(x.toString  + "\n" ))*/

	/*spliter et créer une dataframe*/
		val df = in.map(x => (Wind(x.split(",")(0).trim, x.split(",")(1).trim, x.split(",")(2).trim, x.split(",")(3).trim.toInt, x.split(",")(4).trim.toInt))).toDF()


// afficher les stats standards des colonnes numériques seulement
	df.describe().show()

/*some simple queries*/

df.select("DD").show()

// SQL statements can be run by using the sql methods provided by sqlContext.

df.filter(df("DD") > 100).show()

df.groupBy(("STAID"),("SOUID")).count().show()

df.groupBy(("STAID"),("SOUID")).agg(avg(("DD")), max(("DD")))

df.groupBy("Meter").count().show() /*Tested only with file not with a directory*/

df.write.format("parquet").save("TestDF.parquet")

// lancer des requetes utilisant sqlcontext 

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._
import sqlContext.implicits._

df.registerTempTable("df")

val tt = sqlContext.sql("SELECT * FROM df Where DD>100")

val test = sqlContext.sql("SELECT Meter, year FROM SmartMeter WHERE Value >= 3 AND day <= 10")

/*save data in various format: parquet for example*/
sqlContext.sql("SELECT Meter, year FROM SmartMeter WHERE Value >= 3 AND day <= 10").write.format("parquet").save("namesAndAges.parquet")



////////////////////////////////////////////////
// creation dataframe Source
////////////////////////////////////////////////

	    val inputSource = "/home/user/MySpark/Weather Spark/SourceDD.txt"

	    val inputSourceDD = sc.textFile(inputSource)

// Define the Source schema using a case class.
case class Source(STAID: String, SOUID: String, SOUNAME: String)

/*case class Source(STAID: String, SOUID: String, SOUNAME: String, CN: String, LAT: String, LON: String, HGTH:String, ELEI: String, START:String, STOP: String, PARID: String, PARNAME: String)
*/
//eliminer les header des fichiers de la weather database
val inSource = inputSourceDD.mapPartitionsWithIndex((partitionIdx: Int, lines: Iterator[String]) => {
  if (partitionIdx == 0) {
    lines.drop(23)
  }
  lines  
})

	/*pr tester que le header est bien eliminé*/
	/*inSource.take(4).foreach(x => println(x.toString  + "\n" ))*/

	/*spliter et créer une dataframe*/
		val dfSource = inSource.map(x => (Source(x.split(",")(0).trim, x.split(",")(1).substring(0,6).trim, x.split(",")(2).trim))).toDF()

/*		val dfSource = inSource.map(x => (Source(x.split(",")(0).trim, x.split(",")(1).trim, x.split(",")(2).trim, x.split(",")(3).trim, x.split(",")(4).trim, x.split(",")(5).trim, x.split(",")(6).trim, x.split(",")(7).trim, x.split(",")(8).trim, x.split(",")(9).trim, x.split(",")(10).trim, x.split(",")(11).trim))).toDF()
*/
	dfSource.describe().show()

/*some simple queries*/

	dfSource.select("STAID").show()

// Test jointure entre les deux dataframes

df.filter("DD > 100").join(dfSource, df("STAID") === dfSource("STAID")).withColumn(df("STAID"),df("SOUID"),dfSource("SOUNAME")).groupBy(df("STAID"),df("SOUID")).agg(avg(df("DD")), max(df("DD"))).show

