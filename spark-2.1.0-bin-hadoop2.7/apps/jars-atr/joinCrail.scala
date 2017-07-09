import org.apache.spark.sql.{SaveMode, SparkSession}
spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
val ds1 = spark.read.parquet("crail://flex11-40g0:9060/p1-1000")
val ds2 = spark.read.parquet("crail://flex11-40g0:9060/p2-1000")
val bla = ds1.as("A").joinWith(ds2.as("B"), $"A.randInt" === $"B.randInt")
bla.write.format("parquet").mode(SaveMode.Overwrite).save("crail://flex11-40g0:9060/p-o")
