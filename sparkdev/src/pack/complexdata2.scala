package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object complexdata2 {
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
			.load("file:///C:/data/jv_new.json")
			df.show()

			df.printSchema()
			val flattendf = df.withColumn("Students",expr("explode(Students)"))
			flattendf.show()
			flattendf.printSchema()
			
}
}