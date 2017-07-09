import java.util.Random
import java.nio.ByteBuffer

import org.apache.spark.SparkContext

object GroupByTest {
  def test(numMappers:Int = 10, numKVPairs:Int = 100, valSize:Int = 1024, numReducers:Int = 10) {
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())
  }

  def testDS(numMappers:Int = 10, numKVPairs:Int = 100, valSize:Int = 1024, numReducers:Int = 10) {
   import spark.implicits._
   val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
   val ranGen = new Random
   val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
   val byteArr = new Array[Byte](valSize)
   ranGen.nextBytes(byteArr)
   for (i <- 0 until numKVPairs) {
     arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
    }
    arr1
   }.cache().toDS()
   // this cache ensures that we get the same value for rest of the calculation
   // if we don't do this, we get new value everytime 
  
   println(" ----------> " + pairs1.count)
   val s = System.nanoTime()
   val xx = pairs1.reduce( (a,b) => a )
   val end = System.nanoTime() - s
   println(" Execution time: " + (end / 1000000) + " msecs")
  }

 def testLargeKey(numMappers:Int = 10, numKeys:Int = 100, keySize:Int = 1024, numReducers:Int = 10) {
  import spark.implicits._
  println(s"Starting test with $numMappers, #keys $numKeys, size $keySize, reducers: $numReducers")  
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
  val s = System.nanoTime()
  val gb = pairs1.groupByKey(k => k) // there is only one key 
  val rb = gb.reduceGroups((v1, v2) => {Seq((v1.length + v2.length).toByte).toArray})
  val entries = rb.count
  val end = System.nanoTime() - s
  println(" Execution time: " + (end / 1000000) + " msecs, entries: " + entries )  
 }

def testLargeKeyNoRandom(numMappers:Int = 10, numKeys:Int = 100, keySize:Int = 1024, numReducers:Int = 10) {
  import spark.implicits._
  println(s"Starting test with $numMappers, #keys $numKeys, size $keySize, reducers: $numReducers")
  val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val arr1 = new Array[(Long,Array[Byte])] (numKeys)
      val bb = ByteBuffer.allocate(keySize)   
      for (i <- 0 until numKeys) {
          bb.clear()
          bb.putInt(p.toInt)
          bb.putInt(i)
          bb.clear()
	  val payload = new Array[Byte](keySize)
          bb.get(payload)
          bb.clear()
          arr1(i) = (bb.getLong(), payload)
      }
   arr1
  }.cache()
  // this cache ensures that we get the same value for rest of the calculation
  // if we don't do this, we get new value everytime 

  println(pairs1.count)
  val s = System.nanoTime()
  val gb = pairs1.groupByKey() // there is only one key 
  //val rb = gb.reduceGroups((v1, v2) => v1)
  val rb = gb.map(v => v._1)
  val entries = rb.count
  val end = System.nanoTime() - s
  println(" Execution time: " + (end / 1000000) + " msecs, entries = " + entries)  
 }

}
