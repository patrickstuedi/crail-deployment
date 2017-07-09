import org.apache.spark.sql.{SaveMode, SparkSession}
spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
val ds1 = spark.read.parquet("/p1-R100k-S10k")
val ds2 = spark.read.parquet("/p2-R100k-S10k")
//val bla = ds1.as("A").joinWith(ds2.as("B"), $"A.randInt" === $"B.randInt")
val bla = ds1.joinWith(ds2, ds1("randInt") === ds2("randInt"))
bla.write.format("parquet").mode(SaveMode.Overwrite).save("/p-o")
