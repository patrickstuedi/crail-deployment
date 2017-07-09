import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset
import scala.collection.mutable.StringBuilder

:load ./apps/jars-atr/saveDF.scala 

val hdfs = ""
val crail = "crail://flex11-40g0:9060"
val fs = hdfs
//val file = "/sql/parquet-100m"
val file = "/sql/parquet-20m-s1k-R50m"

val t1 = System.nanoTime()

val df = spark.read.parquet(fs+file)
//val df = spark.read.format("org.apache.spark.sql.execution.datasources.atr.AtrFileFormat").load(fs+file)

saveDF(df, fs+"/xxx")
//discardDF(df, fs+"/xxx")

val t2 = System.nanoTime() 
System.out.println("----------------------------------------")
System.out.println(" load/store  : " + (t2 - t1).toFloat/1000000000 + " sec")
System.out.println("----------------------------------------")

