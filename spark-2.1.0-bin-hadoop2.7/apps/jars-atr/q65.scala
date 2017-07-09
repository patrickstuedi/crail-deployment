// this is my re-writing of q65 

//1G data set
//val location = "/sql/tpcds-data-1G-parquet/"
//val location = "/sql/tmp1/"
//val suffix=".parquet"

//val location = "crail://flex11-40g0:9060/sql/tpcds-data-1G-parquet/"
val location = "crail://flex11-40g0:9060/sql/tmp1/"
val suffix=".parquet"

//1TB daatset 
//val location = "/sql/tpcds-data-1T-parquet/"
//val suffix="-1T.parquet"

// we first read in the tables
val storeSales = spark.read.parquet(location+"store_sales"+suffix)
val dateDim    = spark.read.parquet(location+"date_dim"+suffix)
val store = spark.read.parquet(location+"store"+suffix)
val item = spark.read.parquet(location+"item"+suffix)

val sa1 = storeSales.join(dateDim, storeSales("ss_sold_date_sk") === dateDim("d_date_sk"))
val sa2 = sa1.where($"d_year" === 2001) 
val sa3 =  sa2.groupBy("ss_store_sk", "ss_item_sk") 
val sa4 = sa3.sum("ss_sales_price").withColumnRenamed("sum(ss_sales_price)", "revenue") 

val sc = sa4
val sa = sa4

val sc1 = sc.join(item, sc("ss_item_sk") === item("i_item_sk")) 
val sc2 = sc1.join(store, sc("ss_store_sk") === store("s_store_sk"))

val sb = sa4.groupBy("ss_store_sk").avg("revenue").withColumnRenamed("avg(revenue)", "ave")

val sc3 = sc2.join(sb, sc("ss_store_sk") === sb("ss_store_sk")) 

//val res1 = sc3.where(sc("revenue") <= sb("ave").*(0.1)) // on synthetic data this does not work 

val res1 = sc3.where(sc("revenue") <= sb("ave"))
val res = res1.orderBy("s_store_name", "i_item_desc").select("s_store_name", "i_item_desc", "revenue", "i_current_price", "i_wholesale_cost", "i_brand")

res.printSchema

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
