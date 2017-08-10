import java.nio.ByteBuffer

val size = 1024 * 1024 * 1 
val times  = 16 

val bbArr = new Array[ByteBuffer](times) 
val arrArr = new Array[Array[Byte]](times) 

for ( i <- 0 until times) {
	bbArr(i) = ByteBuffer.allocate(size)
	arrArr(i) = new Array[Byte](size)
}

var sum = 0L
for( i <- 0 until times ) {
	val s = System.nanoTime()
	bbArr(i).get(arrArr(i)) 
	val tx = System.nanoTime() - s
	println("copy[" + i +"] happened in " + tx + " nsec, speed " + size.toFloat*8/(tx.toFloat) + " Gbps ")
	bbArr(i) = null // for GC 
	arrArr(i) = null
	sum+=tx
}
sum/=times
println("---\nResult: copy happened in " + sum + " nsec, speed " + size.toFloat*8/(sum.toFloat) + " Gbps\n---")
