#!/bin/bash

cat  /home/ubuntu/crail-deployment/hadoop/etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh -t $slave 'sudo sh -c "mkfs.ext4 /dev/xvdc"'
        ssh -t $slave 'sudo sh -c "mount /dev/xvdc /mnt/sdc"'
        ssh -t $slave 'sudo sh -c "chown ubuntu /mnt/sdc"'

} < /dev/null; done
