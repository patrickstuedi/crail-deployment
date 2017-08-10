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

val doNull = false
val input = fs+"/sql/parquet-100m"
val output = input+"-sort" 

val key="randInt" 

val ds = spark.read.parquet(input)

val sortedDs = ds.sort(key)

saveDF(sortedDs, output, doNull)
val e = System.nanoTime()
System.out.println("Total time: " + ( e - s) /1000000 + " msec, file written to : " + output )
