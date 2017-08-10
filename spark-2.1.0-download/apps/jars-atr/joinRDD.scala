import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD

val partitions = 10
val keysPerPartition = 10
val showOutput = false

case class KV(index: Long, byteArray: Array[Byte])

def generateRDD():RDD[KV] = {
     spark.sparkContext.parallelize(0 until partitions, partitions).flatMap { p => 
     val arr1 = new Array[KV](keysPerPartition)
     val start = TaskContext.getPartitionId() * keysPerPartition
     val end = start + keysPerPartition
     for (i <- start until end) {
      arr1(i - start) = KV(i, new Array[Byte](i))
     }
     arr1
     }
}

val rdd1 = generateRDD
val rdd2 = generateRDD
/* you need to convert RDD into pair RDD for join */
val rdd1p = rdd1.map(x => (x.index, x.byteArray))
val rdd2p = rdd2.map(x => (x.index, x.byteArray))
/* we can even do our own partitioner */
val joined = rdd1p.join(rdd2p)
/* trigger the compuration */
val count = joined.count
println(count)
require(count == (partitions * keysPerPartition), " count failed")

if(showOutput){
	joined.toDS().show(count.toInt) 
}
