#!/bin/bash

cat  /home/ubuntu/crail-deployment/hadoop/etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh $slave "jps"
} < /dev/null; done
