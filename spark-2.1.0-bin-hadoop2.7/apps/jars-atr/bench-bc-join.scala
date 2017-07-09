import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset
import scala.collection.mutable.StringBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.broadcast

val s = System.nanoTime()

:load apps/jars-atr/saveDF.scala 

val hdfs=""
val crail="crail://flex11-40g0:9060"

//val fs=crail
val fs=hdfs

val f1 = fs+"/sql/parquet-int-1m-s128-R50m"
val f2 = fs+"/sql/parquet-20m-s1k-R50m"
val fo = fs+"/p-o"

// we give the schema 
val schF1 = new StructType().add("intIndex", IntegerType).add("payload", BinaryType)
val schF2 = new StructType().add("randInt", IntegerType).add("randLong", LongType).add("randDouble", DoubleType).add("randFloat", FloatType).add("randString", StringType)

val ds1 = spark.read.schema(schF1).parquet(f1)
val ds2 = spark.read.schema(schF2).parquet(f2)

// always largeDF.join(smallDF)
//val bla = ds1.joinWith(broadcast(ds2), ds1("intIndex") === ds2("randInt")) -> fails with kryo buffer size
val bla = ds1.joinWith(ds2, ds1("intIndex") === ds2("randInt"))

//discardDF(bla, fo)
saveDF(bla, fo)
val e = System.nanoTime()
System.out.println("Total time: " + ( e - s) /1000000 + " msec" )
