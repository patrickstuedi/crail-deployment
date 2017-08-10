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

val fo = fs+"/p-warmup"

val doNull = false
/* start, end, steps, partition */
val x = spark.range(0L, 1000000L, 1L, 10).sort("id")
saveDF(x, fo, doNull)

val e = System.nanoTime()
System.out.println("Total time: " + ( e - s) /1000000 + " msec" )
