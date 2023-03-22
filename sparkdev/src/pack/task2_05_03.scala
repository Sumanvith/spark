package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object task2_05_03 {
    	def main(args:Array[String]):Unit={
			System.setProperty("hadoop.home.dir", "C:\\hadoop")
			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")
			.set("spark.driver.allowMultipleContexts", "true")		
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
			val df  = spark.read.option("header","true").csv("file:///C:/data/df.csv")		
      df.show()
      df.createOrReplaceTempView("df")
      spark.sql("select * from df order by id").show()
      
      println("Like Filter")
      spark.sql("select * from df where product like ('%Gymnastics%')").show()
      
      println("Not Filters")
      spark.sql("select * from df where category != 'Exercise'").show()
      
      println("Not In Filters")
      spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()
      
      println("Null Filters")
      spark.sql("select * from df where product is null").show()
      
      println("Max Function")
      spark.sql("select max(id) from df").show()
      
      println("Min Funtion")
      spark.sql("select min(id) from df").show()
            
    }
}