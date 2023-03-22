package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object dfwfos {
    	def main(args:Array[String]):Unit={
	  
			System.setProperty("hadoop.home.dir", "C:\\hadoop")
			
			
			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")
			.set("spark.driver.allowMultipleContexts", "true")		
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			val csvdf = spark.read.format("csv").option("header",true)
			            .load("file:///C:/data/usdata.csv")
			
			csvdf.show()
			
			csvdf.createOrReplaceTempView("cdf")
			val procdf = spark.sql(" select * from cdf where state='LA' ")
			
			procdf.show()
			procdf.write.format("json").mode("overwrite").save("file:///C:/data/pr")
    }
}