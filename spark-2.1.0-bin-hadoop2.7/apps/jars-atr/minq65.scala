:load ./apps/jars-atr/utils.scala 

// this is my re-writing of q65 
val location = "crail://flex11-40g0:9060/sql/tmp2/"
val suffix=".parquet"

// we first read in the tables
val storeSales = spark.read.parquet(location+"store_sales"+suffix)
val dateDim    = spark.read.parquet(location+"date_dim"+suffix)

val eqJoin = storeSales.join(dateDim, storeSales("ss_sold_date_sk") === dateDim("d_date_sk"))
eqJoin.explain

val lessJoin = storeSales.join(dateDim, storeSales("ss_sold_date_sk") <= dateDim("d_date_sk"))
lessJoin.explain

//sys.exit
