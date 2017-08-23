#!/bin/bash

mnt_dir=$1

cat  etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh $slave "rm -rf /mnt/$mnt_dir/hdfsdir/dfs/data/*"
} < /dev/null; done
