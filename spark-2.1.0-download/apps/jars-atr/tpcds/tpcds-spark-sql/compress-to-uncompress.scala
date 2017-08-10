import org.apache.spark.sql.{SaveMode, SparkSession}

val baseInputDir = "/sql/tpcds-data-1T-parquet/"
val baseOutputDir = "/sql/tpcds-data-1T-parquet-uncompress/"
val tables = Array("customer-1T.parquet", "customer_address-1T.parquet", "customer_demographics-1T.parquet", "date_dim-1T.parquet", "household_demographics-1T.parquet", "inventory-1T.parquet", "item-1T.parquet", "promotion-1T.parquet", "store-1T.parquet", "store_sales-1T.parquet", "time_dim-1T.parquet", "warehouse-1T.parquet" , "web_sales-1T.parquet")

for (item <- tables) {
	val input = baseInputDir + item
	System.out.println( input ) 
	val df = spark.read.parquet(input).toDF
	val output = baseOutputDir + item
	df.write.format("parquet").option("compress", "none").mode(SaveMode.Overwrite).save(output)
}

System.out.println("--------- Done ----------")
