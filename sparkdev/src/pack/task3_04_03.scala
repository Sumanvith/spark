package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
object task3_04_03 {
	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")

			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

			 val df = spark.read
			          .format("xml")
			          .option("rowtag","POSLog")
			          .load("file:///C:/data/transactions.xml")
			          
			 df.printSchema()
	}
}