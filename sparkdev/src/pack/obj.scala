package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object obj {
	def main(args:Array[String]):Unit={
			println("===Started==")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val data =	sc.textFile("file:///C:/data/datatxns.txt")

			val filterdata = data.filter( x => x.contains("Vaulting"))
			
			println("====filterdata data====")
			filterdata.foreach(println)
			println
			println
			
			val concatdata = filterdata.map( x => x+",zeyo")
			
			println("====concatdata data====")
			concatdata.foreach(println)
			println
			println
	}
}