import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.StringBuilder

//warm up
//:load ./apps/jars-atr/query1-debug.scala

object Query1Parquet {

  def execute(spark:SparkSession, id: Int = 1, doNullOutput: Boolean = false) {

    val hdfs = ""
    val crail = "crail://flex11-40g0:9060"
    val fs = crail
    val BASE_DATA_PATH=fs+"/sql/dnb/"
    val DUNS_KEYS=fs+"/sql/dnb/queryKeys"+id+".parquet"
    val LINK_TABLE=fs+"/sql/dnb/DNB_TO_ENTERPRISE_ROWID"
    val RESULT_PATH=fs+"/sql/dnb/databses/"
    val RESULT_NAME="Query1Result"

    val start = System.nanoTime()

    val keyDF = spark.read.parquet(DUNS_KEYS) /* it only has one string col */

    val linkDF = spark.read.parquet(LINK_TABLE)

    val eidDF = keyDF.join(linkDF, keyDF("DUNS_NO") === linkDF("DUNS_NO")).select(linkDF("ENTERPRISE_ID")).dropDuplicates()

    eidDF.cache()

    val dataTables: Array[String] = Array(
      "GARTNER_ESTIMATES_CALCULATED_ROWID",
      "PEERS_ROWID",
      "BMSIW_CONTRACTS_PLUS_ROWID",
      "IBM_REVENUE_UNIFIED_ROWID",
      "IBM_WALLET_SHARE_UNIFIED_ROWID",
      "COMP_INSTALL_BASE_BY_HG_ROWID",
      "FINANCIAL_EXTENDED_V4_ROWID",
      "FIRMOGRAPHICS_DETAILS_ROWID",
      "STRATEGIC_INTENT_ROWID",
      "SERVICE_CONTRACTS_ROWID")


    var i = 0
    val runTimes = new Array[Long](dataTables.length)

    for(tab <- dataTables) {
      val ss = System.nanoTime()

      val fname = BASE_DATA_PATH + tab
      val df = spark.read.parquet(fname)

      val data = df.joinWith(eidDF, df("ENTERPRISE_ID") === eidDF("ENTERPRISE_ID")).drop(eidDF("ENTERPRISE_ID"))

      val tabName = RESULT_PATH + RESULT_NAME + i.toString
      System.out.println("Saving file at : " +  tabName)
      if(doNullOutput){
        data.write.format("org.apache.spark.sql.execution.datasources.atr.AtrFileFormat").mode(SaveMode.Overwrite).save(tabName)
      } else {
        data.write.format("parquet").mode(SaveMode.Overwrite).save(tabName)
      }

      runTimes(i) = System.nanoTime() - ss
      System.out.println("\t [*** " + i + " took : " + runTimes(i).toFloat/1000000 + " msec , has " + 0 + " rows. ***]")
      i = i + 1
    }

    val totalTime:Long = System.nanoTime() - start

    System.out.println("----------------------------------------")
    for(i <- runTimes){
      System.out.println("\texe : " + i.toFloat/1000000 + " msec")
    }
    System.out.println("----------------------------------------")
    var tt:Long = 0
    runTimes.foreach(x => tt+=x)
    System.out.println(" Sum time was   : " + tt.toFloat/1000000000 + " sec")
    System.out.println(" Total time was : " + totalTime.toFloat/1000000000 + " sec")
    System.out.println("----------------------------------------")
  }
}

// THIS IS WITH DUPLICATE
//scala> Query1Parquet.execute(spark, 1, true)
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result0
//  [*** 0 took : 7622.1294 msec , has 48539 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result1
//  [*** 1 took : 1119.0612 msec , has 5 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result2
//  [*** 2 took : 5562.1763 msec , has 1179747 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result3
//  [*** 3 took : 6474.572 msec , has 8823203 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result4
//  [*** 4 took : 2661.5535 msec , has 420858 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result5
//  [*** 5 took : 7068.175 msec , has 7665893 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result6
//  [*** 6 took : 5712.486 msec , has 1334316 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result7
//  [*** 7 took : 10361.504 msec , has 5204074 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result8
//  [*** 8 took : 4580.592 msec , has 4256436 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result9
//  [*** 9 took : 8094.602 msec , has 11587509 rows. ***]
//----------------------------------------
//exe : 7622.1294 msec
//  exe : 1119.0612 msec
//  exe : 5562.1763 msec
//  exe : 6474.572 msec
//  exe : 2661.5535 msec
//  exe : 7068.175 msec
//  exe : 5712.486 msec
//  exe : 10361.504 msec
//  exe : 4580.592 msec
//  exe : 8094.602 msec
//  ----------------------------------------
//Sum time was   : 59.25685 sec
//  Total time was : 59.699856 sec
//  ----------------------------------------
//


//WITHOUT DUPLICATE
//scala> Query1Parquet.execute(spark, 1, true)
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result0
//  [*** 0 took : 4766.0728 msec , has 48539 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result1
//  [*** 1 took : 1113.9292 msec , has 5 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result2
//  [*** 2 took : 5839.2056 msec , has 1179747 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result3
//  [*** 3 took : 6397.355 msec , has 8823203 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result4
//  [*** 4 took : 2652.7532 msec , has 420858 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result5
//  [*** 5 took : 6943.6274 msec , has 7665893 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result6
//  [*** 6 took : 5746.684 msec , has 1334316 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result7
//  [*** 7 took : 8543.11 msec , has 5204074 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result8
//  [*** 8 took : 5319.0117 msec , has 4256436 rows. ***]
//Saving file at : crail://flex11-40g0:9060/sql/dnb/databses/Query1Result9
//  [*** 9 took : 8724.481 msec , has 11587509 rows. ***]
//----------------------------------------
//exe : 4766.0728 msec
//  exe : 1113.9292 msec
//  exe : 5839.2056 msec
//  exe : 6397.355 msec
//  exe : 2652.7532 msec
//  exe : 6943.6274 msec
//  exe : 5746.684 msec
//  exe : 8543.11 msec
//  exe : 5319.0117 msec
//  exe : 8724.481 msec
//  ----------------------------------------
//Sum time was   : 56.04623 sec
//  Total time was : 56.36533 sec
//  ----------------------------------------


// VERDICT: Both are the same
