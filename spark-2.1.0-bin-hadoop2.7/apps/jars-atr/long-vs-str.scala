import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable.ListBuffer

case class AtrLong(l: Long)
case class AtrString(s: String)

val parts = 240
val itemsPerPart = 1000000
val longVal:Long = (1L << 50) - 1
val stringVal:String = "DEADBEEF"
val nullFormat:String = "org.apache.spark.sql.execution.datasources.atr.AtrFileFormat"
val parquetFormat:String = "parquet"
val oformat = parquetFormat

def executeLong(spark: SparkSession) = {
  val start = System.nanoTime()
  import spark.implicits._

  val inputRDD = spark.sparkContext.parallelize(0 until parts, parts).flatMap { p =>
    val base = new ListBuffer[AtrLong]()
    /* now we want to generate a loop and save the parquet file */
    for (a <- 0L until itemsPerPart) {
      base += AtrLong(longVal)
    }
    base
  }
  val outputDS = inputRDD.toDS()
  outputDS.write.format(oformat).mode(SaveMode.Overwrite).save("/test")
  val tt = System.nanoTime() - start
  println("long time: " + tt.toFloat/ 1000000 + " msec")
}

def executeString(spark: SparkSession) = {
  val start = System.nanoTime()
  import spark.implicits._

  val inputRDD = spark.sparkContext.parallelize(0 until parts, parts).flatMap { p =>
    val base = new ListBuffer[AtrString]()
    /* now we want to generate a loop and save the parquet file */
    for (a <- 0L until itemsPerPart) {
      base += AtrString(stringVal)
    }
    base
  }
  val outputDS = inputRDD.toDS()
  outputDS.write.format(oformat).mode(SaveMode.Overwrite).save("/test")
  val tt = System.nanoTime() - start
  println("string time: " + tt.toFloat/ 1000000 + " msec")
}

executeLong(spark)
executeString(spark)
println("warm up done ")
executeLong(spark)
executeString(spark)