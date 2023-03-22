package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object singlecolschemaRDD {

case class schema(id:String,category:String,product:String,mode:String)


def main(args:Array[String]):Unit={

		println("===started===")
		val conf = new SparkConf().setMaster("local[*]").setAppName("first")
		.set("spark.driver.allowMultipleContexts", "true")

		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		val spark = SparkSession
		            .builder()
		            .getOrCreate()

     import spark.implicits._


		val data = sc.textFile("file:///C:/data/datatxns.txt")
		data.take(10).foreach(println)
    val mapsplit = data.map( x => x.split(","))

		val schemardd = mapsplit.map( x => schema(x(0),x(1),x(2),x(3)))
		val filterrdd= schemardd.filter( x => x.product.contains("Gymnastics"))
		println
		println
		filterrdd.foreach(println)

		val df = filterrdd.toDF()

		df.show
}
}
