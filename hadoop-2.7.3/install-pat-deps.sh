#!/bin/bash

cat  /home/ubuntu/crail-deployment/hadoop/etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh -t $slave 'sudo sh -c "apt-get install -y sysstat"'
	#ssh $slave "jps"
} < /dev/null; done
