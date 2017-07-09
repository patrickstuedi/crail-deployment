import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset
import scala.collection.mutable.StringBuilder
import org.apache.spark.sql.types._

val s = System.nanoTime()

:load apps/jars-atr/saveDF.scala 

val hdfs=""
val crail="crail://flex11-40g0:9060"

val fs=crail
//val fs=hdfs

val suffix="-s1k-R50m"
val ff="parquet-20m"

val f1 = fs+"/sql/"+ff+suffix
val f2 = fs+"/sql/"+ff+"2"+suffix
val fo = fs+"/p-o"

// we give the schema 
val sch = new StructType().add("randInt", IntegerType).add("randLong", LongType).add("randDouble", DoubleType).add("randFloat", FloatType).add("randString", StringType)

val ds1 = spark.read.schema(sch).parquet(f1)
val ds2 = spark.read.schema(sch).parquet(f2)

//val jx = ds1.as("A").joinWith(ds2.as("B"), $"A.randInt" === $"B.randInt")
val bla = ds1.joinWith(ds2, ds1("randInt") === ds2("randInt"))

saveDF(bla, fo)
//discardDF(bla, fo)
val e = System.nanoTime()
System.out.println("Total time: " + ( e - s) /1000000 + " msec" )
