// spark-shell script to generate TPC-DS data using Spark 2.x and databricks spark-sql-perf 
// https://github.com/databricks/spark-sql-perf

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import com.databricks.spark.sql.perf.tpcds.TPCDSTables 

// note: need dsdgen executrable on all nodes local file system
//       executor make-dsdgen-slaves.sh script from master
//       or git clone https://github.com/databricks/tpcds-kit
val dsdgen_dir = "/home/ubuntu/crail-deployment/spark-2.1.0-download/apps/dsdgen-dir"

val scale_factor = "10"
val data_location = "/tpcds10"
val format = "parquet"
val overwrite = true
val partitionTables = false
val clusterByPartitionColumns = false
val filterOutNullPartitionValues = false

val tables = new TPCDSTables(sqlContext, dsdgen_dir, scale_factor)
tables.genData(data_location, format, overwrite, partitionTables, clusterByPartitionColumns, filterOutNullPartitionValues)



