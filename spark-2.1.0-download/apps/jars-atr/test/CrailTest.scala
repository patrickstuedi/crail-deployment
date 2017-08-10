import java.nio.ByteBuffer
import java.util.Random
import java.util.concurrent.Future

import com.ibm.crail._
import com.ibm.crail.conf.CrailConfiguration
import com.ibm.crail.memory.OffHeapBuffer

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.CrailBufferedInputStream;
import com.ibm.crail.CrailBufferedOutputStream;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.memory.OffHeapBuffer;
import com.ibm.crail.utils.GetOpt;
import com.ibm.crail.utils.RingBuffer;

import org.apache.spark.SparkEnv 

object CrailTest extends Serializable {

  private val testDir = "/test"

  def createFile(numWriters:Int = 10, size:Long = 1048576L) : Unit = {

    val crailConf = new CrailConfiguration()
    val fs = CrailFS.newInstance(crailConf)
    val testDirExists : Boolean = fs.lookup(testDir).get() != null
    if(testDirExists) {
      // we delete it and create it again
      fs.delete(testDir, true).get().syncDir()
    }
    // on the driver we can create the test directory
    fs.create(testDir, CrailNodeType.DIRECTORY, 0, 0).get().syncDir()
    val execute = sc.parallelize(0 until numWriters, numWriters).foreachPartition( p => {
      //
      import org.apache.spark._
      val crailConf = new CrailConfiguration()
      val fs = CrailFS.newInstance(crailConf)
      // we have a file system in each worker
      val id = SparkEnv.get.executorId
      // file is created
      val file = fs.create(testDir+"/"+id, CrailNodeType.DATAFILE, 0, 0).get().syncDir()
      val output = file.asFile().getBufferedOutputStream(0)
      // size is long, lets do it in multiple of 1MB
      val arr = new Array[Byte](1024*1024)
      val random = new Random(System.nanoTime())
      random.nextBytes(arr)
      val bb = ByteBuffer.wrap(arr)
      var dueSize = size
      while (dueSize > 0) {
        val curSize = Math.min(arr.length, dueSize).toInt
        bb.clear()
        bb.position(curSize)
        bb.flip()
        // bb is ready to be written
        output.write(bb)
        dueSize-=curSize
      }
      output.close()
      fs.close()
    }
    )
  }

  def readFileHeap(numReaders:Int = 10, bufSize:Int = (1024*1024), preferDirect:Boolean = true) : Unit = {
    val execute = sc.parallelize(0 until numReaders, numReaders).foreachPartition( p => {
      //
      import org.apache.spark._
      val crailConf = new CrailConfiguration()
      val fs = CrailFS.newInstance(crailConf)
      // we have a file system in each worker
      val id = SparkEnv.get.executorId
      // get the file
      val file = fs.lookup(testDir+"/"+id).get().asFile()
      val input = file.getBufferedInputStream(file.getCapacity())
      val bb = if(preferDirect) {
        ByteBuffer.allocateDirect(bufSize)
      } else {
        ByteBuffer.allocate(bufSize)
      }
      var read = 0
      var ops:Long = 0
      while (read >= 0) {
        bb.clear()
        read = input.read(bb)
        ops+=1
      }
      System.err.println(" --------> read " + (testDir+"/"+id) + " in " + ops + " operations")
      input.close()
      fs.close()
    }
    )
  }

  def readFileDirect(numReaders:Int = 10, bufSize:Int = (1024*1024), asyncOps:Int = 1) : Unit = {

    val s = System.nanoTime()

    val execute = sc.parallelize(0 until numReaders, numReaders).foreachPartition( p => {
      //
      import org.apache.spark._
      val crailConf = new CrailConfiguration()
      val fs = CrailFS.newInstance(crailConf)
      // we have a file system in each worker
      val id = SparkEnv.get.executorId
      // get the file
      val file = fs.lookup(testDir+"/"+id).get().asFile()
      val input = file.getDirectInputStream(file.getCapacity())

      val bb = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(bufSize))

      var read = 0L
      var ops:Long = 0
      while (read >= 0) {
        bb.clear()
        val fx =  input.read(bb)
        if( fx == null) {
          System.err.println(" ^^^^^^^ hit NULL ")
          read = -1
        } else {
          read = fx.get().getLen
        }
        ops+=1
      }
      System.out.println(" --------> read " + (testDir+"/"+id) + " in " + ops + " operations")
      input.close()
      fs.close()
    }
    )
    val end = System.nanoTime() - s 
    System.out.println(" runtime was : " + ( end / 1000000 ) + " msec ")
  }

  def ioBench(numReaders:Int, bufSize:Int, loop:Int) : Unit = {
   sc.setLogLevel("INFO")

    val start = System.nanoTime()

    val execute = sc.parallelize(0 until numReaders, numReaders).foreachPartition( p => {
       val direct = false
       val size = bufSize 
       val id = SparkEnv.get.executorId
       val filename = testDir+"/"+id
		System.out.println("readSequential, filename " + filename  + ", size " + size + ", loop " + loop + ", direct " + direct);
		val conf:CrailConfiguration = new CrailConfiguration();
		val fs:CrailFS = CrailFS.newInstance(conf);

		var buf:CrailBuffer  = null;
		if (size == CrailConstants.BUFFER_SIZE){
			buf = fs.allocateBuffer();
		} else if (size < CrailConstants.BUFFER_SIZE){
			val _buf = fs.allocateBuffer();
			_buf.clear().limit(size);
			buf = _buf.slice();
		} else {
			buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
		}

		//warmup
		//ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		//bufferQueue.add(buf);
		//warmUp(fs, filename, warmup, bufferQueue);
		
		val file:CrailFile = fs.lookup(filename).get().asFile();
		val bufferedStream:CrailBufferedInputStream = file.getBufferedInputStream(file.getCapacity());
		val directStream:CrailInputStream = file.getDirectInputStream(file.getCapacity());
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		var sumbytes:Double = 0;
		var ops:Double = 0;
                var breakLoop = false
		val start = System.currentTimeMillis();
		while (ops < loop && !breakLoop) {
			if (direct){
				buf.clear();
				val  ret = directStream.read(buf).get().getLen();
				if (ret > 0) {
					sumbytes = sumbytes + ret;
					ops = ops + 1.0;
				} else {
					ops = ops + 1.0;
					if (directStream.position() == 0){
						breakLoop = true
					} else {
						directStream.seek(0);
					}
				}
				
			} else {
				buf.clear();
				val ret = bufferedStream.read(buf.getByteBuffer());
				if (ret > 0) {
					sumbytes = sumbytes + ret;
					ops = ops + 1.0;
				} else {
					ops = ops + 1.0;
					if (bufferedStream.position() == 0){
						breakLoop = true
					} else {
						bufferedStream.seek(0);
					}
				}
			}
		}
		val end = System.currentTimeMillis();
		var executionTime = ((end - start)) / 1000.0;
		var  throughput = 0.0;
		var latency = 0.0;
		var sumbits = sumbytes * 8.0;
		if (executionTime > 0) {
			throughput = sumbits / executionTime / 1000.0 / 1000.0;
			latency = 1000000.0 * executionTime / ops;
		}
		bufferedStream.close();	
		directStream.close();
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.getStatistics().print("close");
		fs.close();
	}
     )
    val end  = System.nanoTime()
    val bw = (loop.toLong * bufSize.toLong) * 8 / (end - start) 
    println("Took " + (end - start) / 1000 + " usec " + bw + " Gbps")
     }
}
