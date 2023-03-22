package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object task1_26_02 {
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
      val df1 = spark.read.option("header","true").csv("file:///C:/data/df1.csv")
      df.show()
      df1.show()
      df.createOrReplaceTempView("df")
      df1.createOrReplaceTempView("df1")
      spark.sql("select * from df order by id").show()
      spark.sql("select * from df1  order by id").show()
      
      println("Select two columns")
      spark.sql("select id,tdate from df  order by id").show()
      
      println("Select column with category filter = Exercise")
      spark.sql("select id,tdate,category from df where category='Exercise' order by id").show()
      
      println("Multi Column filter")
      spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash' ").show()
      
      println("Multi Value Filter")
      spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
    }
}