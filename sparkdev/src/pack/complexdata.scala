package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object complexdata {

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")

			println("================Started1============")

			val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			val df = spark
			.read
			.format("json")
			.option("multiline","true")
			.load("file:///C:/data/jv.json")
			df.show()

			df.printSchema()
			val flattendf = df.select(
					"address.permanentAddress", 
					"address.temporaryAddress", 
					"org", 
					"trainer", 
					"workAddress", 
					"years"
					)
			flattendf.show()
			flattendf.printSchema()

			val flattendf1 = df.withColumn("permanentAddress", expr("address.permanentAddress"))
			.withColumn("temporaryAddress",expr("address.temporaryAddress"))
			.drop("address")

			flattendf1.show()
			flattendf1.printSchema()
	}
}