SPARK_HOME=/home/demo/zac-deployment/spark-2.0.0
EXECUTORS=10
CORES=8
$SPARK_HOME/bin/spark-shell \
 --master yarn-client -Xnojline \
      --num-executors $EXECUTORS \
      --executor-cores $CORES 
      < $SPARK_HOME/apps/tpcds/tpcds-spark-sql/run_tpcds.scala
