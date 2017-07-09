// this is mean to be a small join program when 
// I will have debug on. 
// Author: Animesh Trivedi (atr@zurich.ibm.com) 

import org.apache.spark.sql.{SaveMode, SparkSession}
spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

val f1="/sql/small-100rows-R80-1.parquet"
val f2="/sql/small-100rows-R80-2.parquet"

val ds1 = spark.read.parquet(f1)
val ds2 = spark.read.parquet(f2)
val jx = ds1.joinWith(ds2, ds1("randInt") === ds2("randInt"))
val jxCount = jx.count
jx.write.format("parquet").mode(SaveMode.Overwrite).save("/sql/small-join.parquet")
println("small join has : " + jxCount + " rows" )
