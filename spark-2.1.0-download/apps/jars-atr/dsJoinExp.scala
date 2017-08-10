import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Random
import scala.collection.mutable.ListBuffer

case class Table(randInt:Int, randLong: Long, randDouble: Double, randFloat: Float, randString: String)

object DataGenerator {
  val random = new Random(System.nanoTime())

  def getNextString(size: Int):String = {
    random.alphanumeric.take(size).mkString
  }
  def getNextInt:Int = {
    random.nextInt()
  }
  def getNextInt(max:Int):Int = {
    random.nextInt(max)
  }
  def getNextLong:Long= {
    random.nextLong()
  }
  def getNextDouble:Double= {
    random.nextDouble()
  }

  def getNextFloat: Float = {
    random.nextFloat()
  }
  def getNextByteArray(size: Int):Array[Byte] = {
    val arr = new Array[Byte](size)
    random.nextBytes(arr)
    arr
  }

  def getNextValue(s:String, size: Int): String ={
    getNextString(size)
  }

  def getNextValue(i:Int): Int = {
    getNextInt
  }

  def getNextValue(d:Double): Double = {
    getNextDouble
  }

  def getNextValue(l:Long): Long = {
    getNextLong
  }

}

def myDS(tasks: Int, rowsPerTask: Long, parts:Int) : Dataset[Table] = {
	val inputRDD = spark.sparkContext.parallelize(0 until tasks, tasks).flatMap { p =>
	        val base = new ListBuffer[Table]()
	        for (a <- 0L until rowsPerTask) {
			base += Table(DataGenerator.getNextInt(100),
	            DataGenerator.getNextLong,
        	    DataGenerator.getNextDouble,
	            DataGenerator.getNextFloat,
        	    DataGenerator.getNextString(8))
	        }
        	base
	      }
	val outputDS = if(parts > 0 ) inputRDD.toDS().repartition(parts) else inputRDD.toDS()
	outputDS
}

val ds1 = myDS(80, 10000, 80)
val ds2 = myDS(80, 10000, 80)

val bla = ds1.as("A").joinWith(ds2.as("B"), $"A.randInt" === $"B.randInt")
bla.count 

