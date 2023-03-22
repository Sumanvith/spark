package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
object task1and2_04_03 {
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
			          .option("rowtag","book")
			          .load("file:///C:/data/book.xml")
			          
			 df.show()         
			          			          
					val countryschema = StructType(Array(
					StructField("id",StringType,true),
					StructField("name",StringType,true),
					StructField("check",StringType,true),
					StructField("country", StringType, true)
					))

			val df1 = spark.read
			.format("csv")
			.schema(countryschema)
			.load("file:///C:/data/allcountry1.csv")

			df1.show()

			df1.write.format("csv").partitionBy("country","check").save("file:///C:/data/rcdata2")
				
	}
}