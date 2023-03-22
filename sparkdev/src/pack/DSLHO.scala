package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DSLHO {
  def main(args:Array[String]):Unit={
			System.setProperty("hadoop.home.dir", "C:\\hadoop")
			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")
			.set("spark.driver.allowMultipleContexts", "true")		
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
			val df = spark
			            .read
			            .format("csv")
			            .option("header","true")
			            .load("file:///C:/data/dt.txt")
			
			df.show()
			println("==========Filter Gymnastics===========")
			val filgym = df.filter(   col("category")==="Gymnastics")
			filgym.show()
			println("==========Filter cat Gymnastics spend cash===========")
			val mulcolfilter=df.filter(col("category")==="Gymnastics" &&
			                           col("spendby")==="cash")        
			mulcolfilter.show()
			println("==========ccategory = gymnastics,Exercise===========")
			val mulvalfilter=df.filter(col("category") isin ("Gymnastics","Exercise"))  
			mulvalfilter.show()
			println("==========Product gymnastics===========")
			val likeop = df.filter(col("product") like "%Gymnastics%")
			likeop.show()
			println("==========Product is null===========")
			println
			val nullprod = df.filter(col("product") isNull )
			nullprod.show()
			println("==========Product is Not null===========")
			val nullNOTprod = df.filter(!(col("product") isNull ))
			nullNOTprod.show()
}
}