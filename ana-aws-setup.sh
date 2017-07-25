#!/bin/sh

# Script to setup spark, hadoop, crail environment

## Create directories and mount on Flash
sudo mkfs.ext4 /dev/nvme0n1
sudo mount /dev/nvme0n1 /mnt/nvme0 
sudo chmod a+rw /mnt/nvme0
mkdir /mnt/nvme0/yarn
mkdir /mnt/nvme0/yarn/local-dir
mkdir /mnt/nvme0/hdfsdir
mkdir /mnt/nvme0/hdfsdir/dfs
mkdir /mnt/nvme0/hdfsdir/dfs/data
mkdir /mnt/nvme0/hadoop-tmp

sudo mkfs.ext4 /dev/nvme1n1
sudo mount /dev/nvme1n1 /mnt/nvme1
sudo chmod a+rw /mnt/nvme1
mkdir /mnt/nvme1/yarn
mkdir /mnt/nvme1/yarn/local-dir
mkdir /mnt/nvme1/hdfsdir
mkdir /mnt/nvme1/hdfsdir/dfs
mkdir /mnt/nvme1/hdfsdir/dfs/data
mkdir /mnt/nvme1/hadoop-tmp


## Config for Crail
sudo mkdir -p /mnt/huge
sudo mount -t hugetlbfs nodev /mnt/huge
sudo chmod a+rw /mnt/huge
mkdir /mnt/huge/cachepath
sudo su
echo 4096 > /proc/sys/vm/nr_hugepages
exit

## For disk instances ##
sudo mkfs.ext4 /dev/xvdb
sudo mount /dev/xvdb /mnt/sdb 
sudo chmod a+rw /mnt/sdb
mkdir /mnt/sdb/yarn
mkdir /mnt/sdb/yarn/local-dir
mkdir /mnt/sdb/hdfsdir
mkdir /mnt/sdb/hdfsdir/dfs
mkdir /mnt/sdb/hdfsdir/dfs/data
mkdir /mnt/sdb/hadoop-tmp

sudo mkfs.ext4 /dev/xvdc
sudo mount /dev/xvdc /mnt/sdc
sudo chmod a+rw /mnt/sdc
mkdir /mnt/sdc/yarn
mkdir /mnt/sdc/yarn/local-dir
mkdir /mnt/sdc/hdfsdir
mkdir /mnt/sdc/hdfsdir/dfs
mkdir /mnt/sdc/hdfsdir/dfs/data
mkdir /mnt/sdc/hadoop-tmp




####### Launch Hadoop ##########
## https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html
## the first time, the namenode must be formatted

#	/usr/local/hadoop/bin/hdfs namenode -format 

## if your slaves file is set and SSH enabled to all nodes, then can launch all with the following

#	/usr/local/hadoop/sbin/start-dfs.sh

## start YARN across cluster

#	/usr/local/hadoop/sbin/start-yarn.sh

