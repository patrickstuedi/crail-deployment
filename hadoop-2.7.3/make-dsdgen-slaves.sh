#!/bin/bash

cat  etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh -t $slave 'sudo sh -c "dpkg --configure -a"'	
	ssh -t $slave 'sudo sh -c "apt-get install -y gcc make flex bison byacc git"'	
	ssh $slave 'cd crail-deployment/spark-2.1.0-download/apps/dsdgen-dir; ./make-dsdgen.sh'
} < /dev/null; done
