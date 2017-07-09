/**
 * Usage: BroadcastTest [partitions] [numElem] [blockSize]
 */
object BroadcastTest {
  def test(slices: Int = 10, num: Int = 1000, itr:Int = 4) {
   // values to broadcast 
   
   val arr1 = new Array[Byte](num) 
   val accuLong = sc.longAccumulator("xx")  

    for (i <- 0 until itr) {
      println("=========== iteration " + i)
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to slices, slices).map(_ => (accuLong.add(barr1.value.length))).count  
      println("Iteration %d took %.0f milliseconds, value %d ".format(i, (System.nanoTime - startTime) / 1E6, accuLong.value))
      require(accuLong.value == (slices * arr1.length))
      accuLong.reset()
    }
  }
}
