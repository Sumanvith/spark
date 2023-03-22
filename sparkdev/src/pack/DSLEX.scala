package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object DSLEX {
   def main(args:Array[String]):Unit={
			System.setProperty("hadoop.home.dir", "C:\\hadoop")
			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")
			.set("spark.driver.allowMultipleContexts", "true")		
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
			val df = spark.read.format("csv").option("header","true").csv("file:///C:/data/dt.txt")
			
			df.show()
			
			val tdf = df.selectExpr("id","tdate","split(tdate,'-')[2] as year","amount","category",
			    "product","spendby","case when spendby = 'cash' then 1 else 0 end as status")
			    
			tdf.show()
   }
}