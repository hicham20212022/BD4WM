	import org.apache.spark.rdd.RDD
	import java.text.SimpleDateFormat
	import java.util.{Calendar, Date}
	import scala.util.Random

	 
	    val inputFile = "/home/user/MySpark/WaterSpark/T.txt"

	    /*val inputFile = "/home/user/MySpark/WaterSpark/miniData"*/
	    /*val inputFile = "/home/user/MySpark/WaterSpark/data" */
	    val results = "results"
	    val input = sc.textFile(inputFile) 

/********************************************************************************************/
//Test aggjour avec partition = valeur par défaut = nombre de fichiers dans le directory
// Nombre de fichier est 2246 : 1.4 Go
// nombre d'executeur local[*]
/********************************************************************************************/

	import org.apache.spark.rdd.RDD
	import java.text.SimpleDateFormat
	import java.util.{Calendar, Date}
	import scala.util.Random

	    val inputFile = "/home/user/MySpark/WaterSpark/data"
	    val input = sc.textFile(inputFile) 
	    val parmois = input.map(x => ((x.split(",")(0), x.split(",")(1).substring(3,5).toString, x.split(",")(1).substring(6,10).toString), x.split(",")(3).toInt))

	val resultmois = parmois.combineByKey(
	  (v) => (v, 1),
	  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
	  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
	  ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
	  resultmois.collectAsMap().map(println(_))

/* filtrer en gardant que les valeurs non nulles
	  val parmoisNonNuls = parmois.filter{case (key, value) => value > 0}

/*moyenne par clé*/
	val moyenne =parmois.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)

/********************************************************************************************/
/*Test aggjour avec partition = 2246/2= 1123*/
/* nombre d'executeur local[*]
/* coalesce avec shuffle à true
/********************************************************************************************/
	import org.apache.spark.rdd.RDD
	import java.text.SimpleDateFormat
	import java.util.{Calendar, Date}
	import scala.util.Random

	    val inputFile = "/home/user/MySpark/WaterSpark/data"
	    val input = sc.textFile(inputFile)
	    val input2= input.coalesce(1123, true) 
	    val parmois2 = input2.map(x => ((x.split(",")(0), x.split(",")(1).substring(3,5).toString, x.split(",")(1).substring(6,10).toString), x.split(",")(3).toInt))

	val resultmois2 = parmois2.combineByKey(
	  (v) => (v, 1),
	  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
	  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
	  ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
	  resultmois2.collectAsMap().map(println(_))

/********************************************************************************************/
/*Test aggjour avec partition = 2246/2= 1123*/
/* nombre d'executeur local[*]
/* coalesce avec shuffle à false
/********************************************************************************************/
	import org.apache.spark.rdd.RDD
	import java.text.SimpleDateFormat
	import java.util.{Calendar, Date}
	import scala.util.Random

	    val inputFile = "/home/user/MySpark/WaterSpark/data"
	    val input = sc.textFile(inputFile)
	    val input3= input.coalesce(1123, false) 
	    val parmois3 = input3.map(x => ((x.split(",")(0), x.split(",")(1).substring(3,5).toString, x.split(",")(1).substring(6,10).toString), x.split(",")(3).toInt))

	val resultmois3 = parmois3.combineByKey(
	  (v) => (v, 1),
	  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
	  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
	  ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
	  resultmois3.collectAsMap().map(println(_))



/********************************************************************************************/
/*afficher le contenu des partitions pour voir comment les partitions du RDD ont été créées*/

	    val inputFile = "/home/user/MySpark/WaterSpark/miniData"
	    /*val inputFile = "/home/user/MySpark/WaterSpark/T.txt"*/
	    val input = sc.textFile(inputFile, 6) 
	    val input2= input.coalesce(2)
	        input2.partitions.size
			

  input.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==2) {println(x)}).iterator).collect

  input.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==2) {println(x)}).iterator).count

  input.preferredLocations(inputRawRDD.partitions(1))

/* agreger la valeur par id du compteur */
	    val pairs = input.map(x => (x.split(",")(0), x.split(",")(3).toInt))

	    val pairs2 = input2.map(x => (x.split(",")(0), x.split(",")(3).toInt))

	pairs.mapPartitionsWithIndex( (index: Int, it: Iterator[(String, Int)]) =>it.toList.map(x => if (index ==0) {println(x)}).iterator).collect

	   val test = pairs.reduceByKey((x,y)=>{x+y} )


/* agreger la valeur par id du compteur et jour */

/*prépapre RDD key value */
	    val parjour = inputRawRDD.map(x => ((x.split(",")(0), x.split(",")(1).substring(6,10).toString, x.split(",")(1).substring(3,5).toString, x.split(",")(1).substring(0,2).toString), x.split(",")(3).toInt))

	    val parjour2 = input2.map(x => ((x.split(",")(0), x.split(",")(1).substring(6,10).toString, x.split(",")(1).substring(3,5).toString, x.split(",")(1).substring(0,2).toString), x.split(",")(3).toInt))

parjour.mapPartitionsWithIndex( (index: Int, it: Iterator[((String, String, String, String), Int)]) =>it.toList.map(x => if (index ==0) {println(x)}).iterator).count


	    val parmois = inputRawRDD.map(x => ((x.split(",")(0), x.split(",")(1).substring(3,5).toString, x.split(",")(1).substring(6,10).toString), x.split(",")(3).toInt))
	    val parannee = inputRawRDD.map(x => ((x.split(",")(0), x.split(",")(1).substring(6,10).toString), x.split(",")(3).toInt))


	   val aggparjour = parjour.reduceByKey((x,y)=>{x+y} )
	   val aggparjour2 = parjour2.reduceByKey((x,y)=>{x+y} )

	   val aggparmois = parmois.reduceByKey((x,y)=>{x+y} )
	   val aggparannee = parannee.reduceByKey((x,y)=>{x+y} )

	   /*annee*/
	   /*acceder le premier la somme du premier element */
	   val element1 =aggparannee.take(1)(0)._2
	   val element1 =aggparannee.take(1)(0)._1

	   /*jour*/
	   /*acceder le premier la somme du premier element */
	   val element1 =aggparjour.take(1)




/*calculs statistiques simples*/


/* voir ici http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions*/
/*moyenne par mois et par compteur*/

/* 
1. chaque valeur est transformée en pair (valeur, 1)
2. (label, (sum, count))
3. To compute the average-by-key, I use the map method to divide the sum by the count for each key.
4. Finally, I use the collectAsMap method to return the average-by-key as a dictionary.
*/
	val resultmois = parmois.combineByKey(
	  (v) => (v, 1),
	  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
	  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
	  ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
	  resultmois.collectAsMap().map(println(_))



/* stats par moi et par compteur*/

	val maxvalue = resultmois.values.max
	val minvalue = resultmois.values.min
	val meanvalue = resultmois.values.mean

	val maxKey = resultmois.keys.max
	val minKey = resultmois.keys.min
	val meanKey = resultmois.keys.mean (non valables pour les dates)


/*le compteur et le mois ou la valeur a été la plus élevée*/

        val resultmois.lookup(resultmois.keys.max)



/*************************************/
/* test de SparkSQL et DataFrame*/
/************************************/


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

/* Define the schema using a case class.*/
case class Measure(Meter: String, year: Int, month:Int, day:Int, Interval:Int, Value:Int)

/********************************************************************************************/
Test: Ok
lecture seul fichier
val SmartMeter = sc.textFile("/home/user/MySpark/WaterSpark/T.txt").map(_.split(",")).map(p => Measure(p(0), p(1).substring(6,10).toInt , p(1).substring(3,5).toInt, p(1).substring(0,2).toInt, p(2).trim.toInt ,p(3).trim.toInt)).toDF()

/********************************************************************************************/

/********************************************************************************************/
Test: Ok
lecture repertoire
val SmartMeter = sc.textFile("/home/user/MySpark/WaterSpark/data").map(_.split(",")).map(p => Measure(p(0), p(1).substring(6,10).toInt , p(1).substring(3,5).toInt, p(1).substring(0,2).toInt, p(2).trim.toInt ,p(3).trim.toInt)).toDF()
/********************************************************************************************/


SmartMeter.registerTempTable("SmartMeter")
SmartMeter.show

/*Statistiques de bases*/
/*https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html*/
SmartMeter.describe().show()

/*some simple queries*/


SmartMeter.select("Meter").show()

// SQL statements can be run by using the sql methods provided by sqlContext.

SmartMeter.filter(SmartMeter("year") > 2009).show()
SmartMeter.filter(SmartMeter("Value")>4).show

SmartMeter.groupBy(("Meter"),("year")).count().show()

SmartMeter.groupBy(("Meter"),("year")).agg(avg(("Value")), max(("Value")))

SmartMeter.groupBy("Meter").count().show() /*Tested only with file not with a directory*/

val df = sqlContext.sql("SELECT * FROM SmartMeter Where year=2010")

val test = sqlContext.sql("SELECT Meter, year FROM SmartMeter WHERE Value >= 3 AND day <= 10")

/*save data in various format: parquet for example*/
sqlContext.sql("SELECT Meter, year FROM SmartMeter WHERE Value >= 3 AND day <= 10").write.format("parquet").save("namesAndAges.parquet")


