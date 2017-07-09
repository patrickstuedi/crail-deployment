import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
  * Created by atr on 01.11.16.
  */
object SparkUtils {

  def writeOutDF(df: DataFrame, spark: SparkSession, fname: String = "/tmp"): Unit = {
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(fname)
  }

  def calcCSum(df: DataFrame, spark: SparkSession): Long = {
    val accum = spark.sparkContext.longAccumulator("TableCheckSum")
    df.foreach(f => {
      var csum = 0
      for (i <- 0 until f.length) {
        csum += f(i).hashCode()
      }
      accum.add(csum)
    })
    accum.value
  }

  def countNumPartitions(x:Dataset[_], spark: SparkSession): Long = {
    /* count how many partitions does it have */
    val accum = spark.sparkContext.longAccumulator("resultPartition")
    x.foreachPartition(p => accum.add(1))
    accum.value
  }

  /* implementation of a string accumulator */
  class StringAccumulatorV2 extends AccumulatorV2[String, String] {

    var _string = new String

    override def isZero: Boolean = _string.isEmpty

    override def merge(other: AccumulatorV2[String, String]): Unit = other match {
      case s: StringAccumulatorV2 =>
        _string += s._string
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

    override def copy(): AccumulatorV2[String, String] = {
      val nx = new StringAccumulatorV2
      nx._string = _string
      nx
    }

    override def value: String = _string

    override def add(v: String): Unit = _string +=  v

    override def reset(): Unit = _string = new String
  }

  /* this prints a int col from a passed index */
  def getPartitionInfoWithContent(x:Dataset[_], printableIntIndex: Int, spark: SparkSession): String = {

    val stringAccum = new StringAccumulatorV2()
    spark.sparkContext.register(stringAccum, "stringAccum")
    x.foreachPartition(p => {
      var strx = new String("<")
      p.foreach(item => {
        strx+=item.asInstanceOf[GenericRowWithSchema].getInt(printableIntIndex).toString + " "
      })
      strx+="> makes "
      val str = strx + " partition " + TaskContext.getPartitionId() + " , which is executor ID: " + SparkEnv.get.executorId + " | \n"
      stringAccum.add(str)})
    stringAccum.value
  }

  def getPartitionInfo(x:Dataset[_], spark: SparkSession): String = {
    val stringAccum = new StringAccumulatorV2()
    spark.sparkContext.register(stringAccum, "stringAccum")
    x.foreachPartition(p => {
      val str = " partition " + TaskContext.getPartitionId() + " is at executor ID: " + SparkEnv.get.executorId + " | \n"
      stringAccum.add(str)})
    stringAccum.value
  }

  def bench(res: Dataset[Row], count:Boolean = false) : Unit = {
    val start = System.nanoTime()
    if(count)
      println(" result rows: " + res.count)
    else
      res.collect
    val end = System.nanoTime()
    val msec = (end -start)/1000000
    val msecF = msec.asInstanceOf[Float]
    println("*******************************")
    println("Execution took : " + msecF/1000 + " sec " )
    println("*******************************")
  }

  def bench100(res: Dataset[Row], count:Boolean = false) : Unit = {
    bench(res.limit(100), count)
  }
}
