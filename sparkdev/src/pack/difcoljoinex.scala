package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object difcoljoinex {

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "C:\\hadoop")

			println("================Started1============")


			val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			val df1 = spark.read.format("csv")
			.option("header","true")
			.load("file:///C:/data/join1.csv")


			df1.show()

			val df2 = spark.read.format("csv")
			.option("header","true")
			.load("file:///C:/data/join2_new.csv")


			df2.show()



			println
			println("=====Inner Join=====")
			println


			val injoindf = df1.join( df2 ,  Seq("txnno"),  "inner")

			injoindf.show()


			println
			println("=====left Join=====")
			println


			val leftjoin = df1.join( df2 ,  Seq("txnno"),  "left")

			leftjoin.show()


			println
			println("=====right Join=====")
			println


			val rightjoin = df1.join( df2 ,  Seq("txnno"),  "right")
			                

			rightjoin.show()
			
			
			println
			println("=====Full Join=====")
			println


			val fulljoin = df1.join(df2 , Seq("txnno"),"full")
			                  .orderBy("txnno")
			
			
			fulljoin.show()
			
			println
			println("=====Different Column Join=====")
			println
			
			val df3 = spark.read.format("csv").option("header","true").load("file:///C:/data/join2.csv")
			
			val joindf = df1.join(df3, df1("txnno")===df3("tno"),"inner").drop("tno")
			
			joindf.show()
			
			println
			println("====Left anti=====")
			println
			
			val antijoin = df1.join(df2,Seq("txnno"),"left_anti")
			
			antijoin.show()
			
			println
			println("====cross anti=====")
			println
			
			
			val crossjoin = df1.crossJoin(df2)
			
			crossjoin.show()
			
			println
			println("==== List =====")
			println
			
			val listin = df1.select("txnno").rdd.map( x => x.mkString("")).collect().toList
			
			print(listin)
			
			println
			println("==== Ambiguity =====")
			println
			
      val procdf = df1.filter($"txnno".isin(listin: _*))
      
      procdf.show()
      
}
}