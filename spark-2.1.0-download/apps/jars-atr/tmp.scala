import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.StringBuilder

    val hdfs = ""
    val crail = "crail://flex11-40g0:9060"
    val fs = crail
    val BASE_DATA_PATH=fs+"/sql/dnb/"
    val DUNS_KEYS=fs+"/sql/dnb/queryKeys"+1+".parquet"
    val LINK_TABLE=fs+"/sql/dnb/DNB_TO_ENTERPRISE_ROWID"
    val RESULT_PATH=fs+"/sql/dnb/databses/"
    val RESULT_NAME="Query1Result"


    val keyDF = spark.read.parquet(DUNS_KEYS)

    keyDF.createOrReplaceTempView("DUNS")

    val linkDF = spark.read.parquet(LINK_TABLE)

    linkDF.createOrReplaceTempView("LINK")

    val linkQuery = """
            select distinct LINK.ENTERPRISE_ID from DUNS
            join LINK on DUNS.DUNS_NO=LINK.DUNS_NO
                    """
    val eidDF = spark.sql(linkQuery)
    eidDF.persist(StorageLevel.MEMORY_ONLY)
    eidDF.createOrReplaceTempView("EID")

