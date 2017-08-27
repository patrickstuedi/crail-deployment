#!/bin/bash

cat  etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh $slave 'cd crail-deployment/spark-2.1.0-download/apps/dsdgen-dir; ./make-dsdgen.sh'
} < /dev/null; done
