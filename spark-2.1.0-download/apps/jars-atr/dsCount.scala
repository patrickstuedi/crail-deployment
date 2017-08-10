import org.apache.spark.sql.{Dataset, SparkSession}

val rows:Long = 8000000000L
val partitions = 10
val inputDS = spark.range(0L, rows).repartition(partitions)
val count = inputDS.count

/* options: To count number of partitions */
def countNumPartitions(spark: SparkSession, x:Dataset[_]): Long = {
    /* count how many partitions does it have */
    val accum = spark.sparkContext.longAccumulator("resultPartition")
    x.foreachPartition(p => accum.add(1))
    accum.value
}
val countP = countNumPartitions(spark, inputDS)
