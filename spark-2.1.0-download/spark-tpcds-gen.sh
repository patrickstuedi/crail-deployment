#!/bin/bash

echo "Generate TPC-DS data..."
echo "usage: ./spark-tpcds-gen.sh -n NUM_EXECUTORS -c EXECUTOR_CORES -e EXECUTOR_MEMORY -d DRIVER_MEMORY"
echo "       defaults: -n 8, -c 4, -m 24G, -d 48G"

NUM_EXECUTORS=8
EXECUTOR_CORES=4
EXECUTOR_MEMORY="24G"
DRIVER_MEMORY="48G"

while getopts ":n:c:e:d:" opt; do
  case $opt in
    n) NUM_EXECUTORS="$OPTARG"
    ;;
    c) EXECUTOR_CORES="$OPTARG"
    ;;
    e) EXECUTOR_MEMORY="$OPTARG"
    ;;
    d) DRIVER_MEMORY="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

./bin/spark-shell -i tpcds-gen.scala --master yarn --deploy-mode client --num-executors $NUM_EXECUTORS --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEMORY --driver-memory $DRIVER_MEMORY --jars apps/jars-ana/spark-sql-perf-assembly-0.5.0-SNAPSHOT.jar



