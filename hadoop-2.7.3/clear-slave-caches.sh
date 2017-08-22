#!/bin/bash

cat  /home/ubuntu/crail-deployment/hadoop/etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh -t $slave 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches && free"'
} < /dev/null; done
