import java.util.Random

import org.apache.spark.SparkContext

val numMappers:Int = 10
val numKeys:Int = 10
val keySize:Int = 1024
val numReducers:Int = 10 

import spark.implicits._
val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
val ranGen = new Random(System.nanoTime())
val arr1 = new Array[Array[Byte]] (numKeys)
for (i <- 0 until numKeys) {
     val key = new Array[Byte](keySize)
     ranGen.nextBytes(key)
     arr1(i) = key 
 }
 arr1
}.cache().toDS()
// this cache ensures that we get the same value for rest of the calculation
// if we don't do this, we get new value everytime 

println(pairs1.count)
val gb = pairs1.groupByKey(k => k) // there is only one key 
gb.count.show
val rb = gb.reduceGroups((v1, v2) => {Seq((v1.length + v2.length).toByte).toArray})
