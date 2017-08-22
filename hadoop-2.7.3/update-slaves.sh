#!/bin/bash

cat  etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh $slave 'cd crail-deployment; git pull'
} < /dev/null; done
