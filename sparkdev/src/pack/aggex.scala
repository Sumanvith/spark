package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object aggex {


def main(args:Array[String]):Unit={

		System.setProperty("hadoop.home.dir", "C:\\hadoop")


		val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df1 = spark.read.format("csv").option("header","true").load("file:///C:/data/aggt.csv")
		df1.show()
		
		val aggres = df1.groupBy("name").agg(sum("amt").cast(IntegerType).as("Total")).orderBy("name")
		
		aggres.show()
}

}