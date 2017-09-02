#!/bin/bash

cat  /home/ubuntu/crail-deployment/hadoop/etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh -t $slave 'sudo sh -c "cp ~/crail-deployment/hadoop/rc.local-r4 /etc/rc.local"'

} < /dev/null; done
