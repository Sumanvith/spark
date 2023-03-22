package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
object awscon {
	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")


			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

			val sqldf = spark.read
			.format("jdbc")
			.option("url","jdbc:mysql://zeyodb.cz7qhc39aphp.ap-south-1.rds.amazonaws.com:3306/zeyodb")
			.option("driver","com.mysql.jdbc.Driver")
			.option("user","root")
			.option("password","Aditya908")
			.option("dbtable","cashdata")
			.load()
			
			sqldf.show()	
	}
}
