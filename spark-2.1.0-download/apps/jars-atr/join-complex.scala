import org.apache.spark.sql.{SaveMode, SparkSession}
spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

def stringSum(str: String): Int = {
	var count = 0;
	str.foreach( i => (count+=i.toInt))
	count
}

val f1 = "/p1-R500k-S10k"
val f2 = "/p2-R500k-S10k"

val ds1 = spark.read.parquet(f1)
val ds2 = spark.read.parquet(f2)
//val bla = ds1.as("A").joinWith(ds2.as("B"), $"A.randInt" === $"B.randInt")

// where randInt is the same 
val step1 = ds1.joinWith(ds2, ds1("randInt") === ds2("randInt"))
// and the first table has the bigger long 
val step2 = step1.filter($"_1.randLong" > $"_2.randLong")
// and the second table has bigger double 
val step3 = step2.filter($"_2.randDouble" > $"_1.randDouble")
// and the first table has a float > 0.5 and the second one has lower than 0.5 
val step4 = step3.filter($"_1.randFloat" > 0.5 and $"_2.randFloat" < 0.5)
// we now only select the key and the string 
//val step5 = step4.map ( k => (k._1.getInt(0), (k._1.getString(4) + k._2.getString(4)).toUpperCase))
val step5 = step4.map ( k => (k._1.getInt(0), stringSum(k._1.getString(4) + k._2.getString(4))))

step5.write.format("parquet").mode(SaveMode.Overwrite).save("/p-o")
