package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
object task1_05_03 {
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
			          .format("csv")
			          .option("header","true")
			          .load("file:///C:/data/dt.txt")
			
			val df1 = df.select("id", "tdate","category")
			
			df1.show()
			
			df1.createOrReplaceTempView("result")
			
			val resDSL= spark.sql("select *, 1 as status from result")
			
			resDSL.show()
	}
}