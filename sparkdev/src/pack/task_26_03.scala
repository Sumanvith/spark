package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source


object task_26_03 {

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")

			println("================Started1============")

			val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			//  ==== SCALA URL Consumption

			val html = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
			val urldata = html.mkString
						
			//  ==== String - RDD
			
			val rdd = sc.parallelize(List(urldata))

			//  ==== JSON RDD - df
			
			val df = spark.read.json(rdd)

			df.show()

			df.printSchema()

			val flat1 = df.withColumn("results", expr("explode(results)"))
			flat1.show
			flat1.printSchema()
			val finalflatten= flat1.select(
					"nationality",
					"results.user.cell",
					"results.user.dob",
					"results.user.email",
					"results.user.gender",
					"results.user.location.city",
					"results.user.location.state",
					"results.user.location.street",
					"results.user.location.zip",
					"results.user.md5",
					"results.user.name.first",
					"results.user.name.last",
					"results.user.name.title",
					"results.user.password",
					"results.user.phone",
					"results.user.picture.large",
					"results.user.picture.medium",
					"results.user.picture.thumbnail",
					"results.user.registered",
					"results.user.salt",
					"results.user.sha1",
					"results.user.sha256",
					"results.user.username",
					"seed",
					"version"
					)
			finalflatten.show()
			finalflatten.printSchema()
	}
}