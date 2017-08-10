#!/bin/bash 

if [ $# -lt 1 ]; 
then 
	echo " $0 jarfile classname [args] " 
	exit 1
fi 

jar=$1
classname=$2
shift 2

java -cp $jar:/home/demo/zac-deployment/hadoop-2.6.0/etc/hadoop:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/common/lib/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/common/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/hdfs:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/hdfs/lib/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/hdfs/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/yarn/lib/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/yarn/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/mapreduce/lib/*:/home/demo/zac-deployment/hadoop-2.6.0/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:$CRAIL_HOME/jars/* $classname $@ 
