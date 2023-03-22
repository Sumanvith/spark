package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object singlecolrowRDD {
def main(args:Array[String]):Unit={

		println("===started===")
		val conf = new SparkConf().setMaster("local[*]").setAppName("first")
		.set("spark.driver.allowMultipleContexts", "true")

		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession
		.builder()
		.getOrCreate()

		import spark.implicits._
		
		val structschema = StructType(Array(
				StructField("id",StringType),
				StructField("category",StringType),
				StructField("product",StringType),
				StructField("mode", StringType)
				))
		
		val data = sc.textFile("file:///C:/data/datatxns.txt")
		data.take(10).foreach(println)
		val mapsplit = data.map( x => x.split(","))
    println(mapsplit.collect.toList.size)
		val rowrdd = mapsplit.map( x => Row(x(0),x(1),x(2),x(3)))
		val filterdata = rowrdd.filter (x => x(2).toString().contains("Gymnastics"))
		filterdata.foreach(println)
    val df = spark.createDataFrame(filterdata, structschema)
    df.show
}
}