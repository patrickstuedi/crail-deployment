#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 mnt_dir" >&2
  exit 1
fi


mnt_dir=$1

cat  etc/hadoop/slaves | while read slave
do {
  	echo "ssh into $slave"
	ssh $slave "rm -rf /mnt/$mnt_dir/hdfsdir/dfs/data/*"
} < /dev/null; done
