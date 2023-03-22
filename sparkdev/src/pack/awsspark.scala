package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
object awsspark {
	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")


			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

			.set("spark.driver.allowMultipleContexts", "true")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			
			
		
			
			val spark = SparkSession
			                .builder
			                .config("fs.s3a.access.key","AKIASVGFNNNDOTIRMUEP")
			                .config("fs.s3a.secret.key","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")
			                .getOrCreate()

			import spark.implicits._
      val countryschema = StructType(Array(
			    
					StructField("txnno",StringType,true),
					StructField("txndate",StringType,true),
					StructField("custno",StringType,true),
					StructField("amount", StringType, true),
					StructField("category",StringType,true),
					StructField("product",StringType,true),
					StructField("city",StringType,true),
					StructField("state",StringType,true),
					StructField("spendby", StringType, true)
					))


     
			val df = spark.read.format("csv")
					        .schema(countryschema)
					         .load("s3a://zeyoathenabucket/txns10k.txt")
			df.show()
					
	}
}
