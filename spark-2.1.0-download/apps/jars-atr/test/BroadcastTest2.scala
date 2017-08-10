/**
  * Usage: BroadcastTest [partitions] [numElem] [blockSize]
  */
import org.apache.spark.broadcast.Broadcast

import scala.util.Random

import java.io._
import java.io.File

import java.lang.Long

object BroadcastTest {
  def test(tasks: Int = 10, num: Int = 10000, size:Int = 4096 , itr:Int = 4) {
    val ranx = new Random()
    val sb = StringBuilder.newBuilder
    // values to broadcast
    val toBroadcastArr = new Array[Array[Byte]](num)
    // these are variables that will be broadcast'ed - we generate them
    for( a <- 0 until num){
      toBroadcastArr(a) = new Array[Byte](size) // 4k
      ranx.nextBytes(toBroadcastArr(a))
    }

    val accuLong = sc.longAccumulator("counter")

    val broadcastVariableArr = new Array[Broadcast[Array[Byte]]](num)
    var sumRunTime = 0L
    var sumWriteTime = 0L
    println(s"Config: doing a broadcast test with $tasks tasks, $num broadcasts, of $size bytes, for $itr iteration")

    for (i <- 0 until itr) {
      println("starting iteration " + i + " ...")
      val startTime = System.nanoTime
      for( a <- 0 until num){
        // do all the broadcasts
        broadcastVariableArr(a) = sc.broadcast(toBroadcastArr(a))
      }
      sumWriteTime +=(System.nanoTime - startTime)
      val observedSizes = sc.parallelize(1 to tasks, tasks).map(_ => {
        // for each partition we need to read all broadcast variables
        var sizeSum = 0L
        for (a <- 0 until num) {
          sizeSum+=(broadcastVariableArr(a).value.length)
        }
        (accuLong.add(sizeSum))
      }).count
      val runTimeThisItr = System.nanoTime - startTime
      sumRunTime+=runTimeThisItr
      sb.append(s"Iteration %d took %.0f milliseconds, value %d \n".format(i, runTimeThisItr / 1E6, accuLong.value))
      accuLong.reset()
    }
    println(sb.mkString)
    println(" on average it takes : " + (sumRunTime / (1000 * itr * num)) +
      " usecs, writeBroadcast is around : " + (sumWriteTime / (1000 * itr * num)) + " usec")
  }

  def testWithWriteProfile(tasks: Int = 10, num: Int = 10000, size:Int = 4096 , itr:Int = 4) {
    val ranx = new Random()
    val sb = StringBuilder.newBuilder
    // values to broadcast
    val toBroadcastArr = new Array[Array[Byte]](num)
    // these are variables that will be broadcast'ed
    for( a <- 0 until num){
      toBroadcastArr(a) = new Array[Byte](size) // 4k
      ranx.nextBytes(toBroadcastArr(a))
    }

    val accuLong = sc.longAccumulator("counter")

    val broadcastVariableArr = new Array[Broadcast[Array[Byte]]](num)
    var sumRunTime = 0L
    var sumWriteTime = 0L
    val writeTimeStamps = new Array[Long](num * itr)
    println(s"Config: doing a broadcast test (writeProfile) with $tasks tasks, $num broadcasts, of $size bytes, for $itr iteration")

    for (i <- 0 until itr) {
      println("starting iteration " + i + " ...")
      val startTime = System.nanoTime
      for( a <- 0 until num){
        val s = System.nanoTime()
        // do all the broadcasts
        broadcastVariableArr(a) = sc.broadcast(toBroadcastArr(a))
        writeTimeStamps((i * num) + a ) = (System.nanoTime() - s)
      }
      sumWriteTime +=(System.nanoTime - startTime)
      val observedSizes = sc.parallelize(1 to tasks, tasks).map(_ => {
        // for each partition we need to read all braodcast varaibles
        var sizeSum = 0L
        for (a <- 0 until num) {
          sizeSum+=(broadcastVariableArr(a).value.length)
        }
        (accuLong.add(sizeSum))
      }).count
      val runTimeThisItr = System.nanoTime - startTime
      sumRunTime+=runTimeThisItr
      sb.append(s"Iteration %d took %.0f milliseconds, value %d \n".format(i, runTimeThisItr/ 1E6, accuLong.value))
      accuLong.reset()
    }
    println(sb.mkString)
    println(" on average it takes : " + (sumRunTime / (1000 * itr * num)) +
      " usecs, writeBroadcast is around : " + (sumWriteTime / (1000 * itr * num)) + " usec")

    val file = new File("./latencies.data")
    val bw = new BufferedWriter(new FileWriter(file))
    for ( lat <- 0 until (num * itr)) {
      bw.write( lat + " " + writeTimeStamps(lat)/1000 + " usec \n")
    }
    bw.close()
    println("wrote the latency file (./latencies.data) as well ")
  }
}

println("\n\n")
println("Welcome to the Broadcast Test. You have two options")
println("1. def test(tasks: Int = 10, num: Int = 10000, size:Int = 4096 , itr:Int = 4)")
println("   this test does the basic broadcast testing and prints the average time\n " +
  "   for writing and reading")
println("2. def testWithWriteProfile(tasks: Int = 10, num: Int = 10000, size:Int = 4096 , itr:Int = 4) ")
println("   this test is same as above, but it writes individual write latencies in a local\n" +
  "   file (latencies.dat)\n ")
