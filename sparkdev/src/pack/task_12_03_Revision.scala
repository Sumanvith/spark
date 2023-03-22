package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object task_12_03_Revision {


case class schema(
		txnno:String,
		txndate:String,
		custno:String,
		amount:String,
		category:String,
		product:String,
		city:String,
		state:String,
		spendby:String)


def main(args:Array[String]):Unit={

		System.setProperty("hadoop.home.dir", "C:\\hadoop")



		val colist = List("txnno",
				"txndate",
				"custno",
				"amount",
				"category",
				"product",
				"city",
				"state",
				"spendby")




		println("================Started1============")


		val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._



		val data = sc.textFile("file:///C:/data/revdata/file1.txt")

		data.take(5).foreach(println)




		val gymdata= data.filter( x => x.contains("Gymnastics"))

		println
		println("===Gymdata===")
		println
		gymdata.take(5).foreach(println)





		val mapsplit = gymdata.map( x => x.split(","))

		val schemardd = mapsplit.map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		val prodfilter = schemardd.filter( x => x.product.contains("Gymnastics"))

		println
		println("===Gymdata prod===")
		println


		prodfilter.take(5).foreach(println)



		println
		println("===schema rdd to dataframe===")
		println


		val schemadf = prodfilter.toDF().select(colist.map(col): _*)

		schemadf.show(5)





		val file2 = sc.textFile("file:///C:/data/revdata/file2.txt")


		val mapsplit1=file2.map( x => x.split(","))


		val rowrdd = mapsplit1.map( x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		println
		println("===row rdd===")
		println
		println


		rowrdd.take(5).foreach(println)



		val structschema = StructType(Array(
				StructField("txnno",StringType,true),
				StructField("txndate",StringType,true),
				StructField("custno",StringType,true),
				StructField("amount", StringType, true),
				StructField("category", StringType, true),
				StructField("product", StringType, true),
				StructField("city", StringType, true),
				StructField("state", StringType, true),
				StructField("spendby", StringType, true)
				))



		println
		println("===row df===")
		println
		println




		val rowdf = spark.createDataFrame(rowrdd, structschema).select(colist.map(col): _*)

		rowdf.show(5)




		val csvdf = spark.read.format("csv").option("header","true")
		.load("file:///C:/data/revdata/file3.txt").select(colist.map(col): _*)

		println
		println("===csv df===")
		println

		csvdf.show(5)





		val jsondf = spark.read.format("json").load("file:///C:/data/revdata/file4.json")
		.select(colist.map(col): _*)
		println
		println("===json df===")
		println

		jsondf.show(5)


		val parquetdf = spark.read.load("file:///C:/data/revdata/file5.parquet")
		.select(colist.map(col): _*)
		println
		println("===parquet df===")
		println

		parquetdf.show(5)


		val xmldf = spark.read.format("xml").option("rowtag","txndata")
		.load("file:///C:/data/revdata/file6")
		.select(colist.map(col): _*)

		println
		println("===xmldf===")
		println

		xmldf.show(5)




		val uniondf = schemadf
		.union(rowdf)
		.union(csvdf)
		.union(jsondf)
		.union(parquetdf)
		.union(xmldf)


		println
		println("===uniondf===")
		println

		uniondf.show(5)





		val procdf = uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
		.withColumnRenamed("txndate", "year")
		.withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
		.filter(col("txnno")>50000)


		println
		println("===procdf===")
		println

		procdf.show(5)




		procdf.write.mode("append").partitionBy("category").save("file:///C:/data/finalpdata")
		
		
		println("===revision complete==")
		




		/*val conf = new SparkConf().setAppName("revision").setMaster("local[]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._


			val df = spark
			            .read
			            .format("csv")
			            .option("header","true")
			            .load("file:///C:/data/dt.txt")

			df.show()



			val tdf =  df.withColumn(  "year"   ,  expr("split(tdate,'-')[2]")   )
			             .withColumnRenamed("tdate", "year")
			             .withColumn("category", expr("substring(category,1,4)"))
			             .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))

			tdf.show()*/

}

}