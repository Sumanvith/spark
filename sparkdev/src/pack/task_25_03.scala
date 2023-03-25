package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object task_25_03 {
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
			.load("file:///C:/data/cake.json")
			df.show()
			df.printSchema()
			
			val flattendf = df.select(
			    
			                            "id",
			                            "image.height",
			                            "image.url",
			                            "image.width",
			                            "name",
			                            "thumbnail.height1",
			                            "thumbnail.url1",
			                            "thumbnail.width1",
			                            "type"
			            )
			flattendf.show()
			flattendf.printSchema()
			
			val finalcomplexdf = flattendf.select(
			                                  col("id"),
			                                  col("type"),
			                                  col("name"),
			                                  struct(
			                                        col("url"),
			                                        col("width"),
			                                        col("height"),
			                                        col("url1"),
			                                        col("width1"),
			                                        col("height1")
			                                        ).as("allfields")
			                )
			finalcomplexdf.show()
			finalcomplexdf.printSchema()
}
}