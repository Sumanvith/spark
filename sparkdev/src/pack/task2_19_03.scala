package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object task2_19_03 {

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")

			println("================Started1============")


			val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			val df1 = spark.read.format("csv")
			.option("header","true")
			.load("file:///C:/data/join1.csv")


			df1.show()

			val df2 = spark.read.format("csv")
			.option("header","true")
			.load("file:///C:/data/join2_new.csv")

			df2.show()

			println
			println("=====Left Semi Join=====")
			println
			
			val leftsemijoin = df1.join( df2 ,  Seq("txnno"),  "left_semi")
			                
			leftsemijoin.show()

      
}
}