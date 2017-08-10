val hdfs = "hdfs://flex11-40g0:9000"
val rdfs = "crail://flex11-40g0:9060"

val tpcds_csv_dir = "/sql/1G"
val tpcds_csv_suffix = ".dat"
val tpcds_common_dir = "/sql/1G-parquet"
val tpcds_hdfs_dir = tpcds_common_dir
val tpcds_rdfs_dir = rdfs + "/" + tpcds_common_dir
val tpcds_parquet_suffix = ".parquet"
val tpcds_SF = ""
val tpcds_query_dir = "/home/demo/zac-deployment/spark-2.0.0/apps/tpcds/tpcds-spark-sql/queries"
val tpcds_query_suffix = ".sql"
