import org.apache.spark.sql.{Dataset, SparkSession}

def warmUp(cores: Long = 100L, keysPerCore: Long = 1L) : Unit = {
	val t = cores * keysPerCore
	val ds1 = spark.range(0, t)
	val ds2 = spark.range(0, t)
	val res = ds1.join(ds2) 
	res.count 
}
