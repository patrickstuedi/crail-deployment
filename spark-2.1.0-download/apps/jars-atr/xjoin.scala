// this is my re-writing of q65 

//val location = "crail://flex11-40g0:9060/sql/tmp2/"
val location = "/sql/tmp2/"
val suffix=".parquet"

val storeSales = spark.read.parquet(location+"store_sales"+suffix)
val dateDim    = spark.read.parquet(location+"date_dim"+suffix)

val jall = storeSales.join(dateDim)
val jcond = storeSales.join(dateDim, storeSales("ss_sold_date_sk") === dateDim("d_date_sk"))

println("-------------------------------")
println("val \"res\" is the final variable ")
println("you can call show or collect to execute it")
println("OR call bench(res), and I will show you time for collect")
println("-------------------------------")

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

def bench(res: Dataset[Row], count:Boolean = false) : Unit = {
	val start = System.nanoTime()
	if(count)
		println(" result rows: " + res.count)
	else 
		res.collect
	val end = System.nanoTime()
	val msec = (end -start)/1000000
	val msecF = msec.asInstanceOf[Float]
	println("*******************************")
	println("Q65 took : " + msecF/1000 + " sec " )
	println("*******************************")
}

def bench100(res: Dataset[Row], count:Boolean = false) : Unit = {
	bench(res.limit(100), count)
}
