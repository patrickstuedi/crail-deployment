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

sudo mkfs.ext4 /dev/nvme2n1
sudo mount /dev/nvme2n1 /mnt/nvme2
sudo chmod a+rw /mnt/nvme2
mkdir /mnt/nvme2/yarn
mkdir /mnt/nvme2/yarn/local-dir
mkdir /mnt/nvme2/hdfsdir
mkdir /mnt/nvme2/hdfsdir/dfs
mkdir /mnt/nvme2/hdfsdir/dfs/data
mkdir /mnt/nvme2/hadoop-tmp

sudo mkfs.ext4 /dev/nvme3n1
sudo mount /dev/nvme3n1 /mnt/nvme3
sudo chmod a+rw /mnt/nvme3
mkdir /mnt/nvme3/yarn
mkdir /mnt/nvme3/yarn/local-dir
mkdir /mnt/nvme3/hdfsdir
mkdir /mnt/nvme3/hdfsdir/dfs
mkdir /mnt/nvme3/hdfsdir/dfs/data
mkdir /mnt/nvme3/hadoop-tmp


## cgroups for memory limits
sudo mount -t tmpfs none /sys/fs/cgroup
sudo mkdir /sys/fs/cgroup/memory
sudo mount -t cgroup none /sys/fs/cgroup/memory -o memory
sudo mkdir /sys/fs/cgroup/memory/0
sudo chmod a+rw /sys/fs/cgroup/memory/0
sudo chmod a+rw /sys/fs/cgroup/memory/0/*
echo 120G > /sys/fs/cgroup/memory/0/memory.limit_in_bytes
cat /sys/fs/cgroup/memory/0/memory.limit_in_bytes
# echo pid > /sys/fs/cgroup/memory/0/tasks
for i in `jps | grep CoarseGrainedExecutorBackend | awk '{print $1}'`; do echo $i > /sys/fs/cgroup/memory/0/tasks; done
for i in `jps | grep DataNode | awk '{print $1}'`; do echo $i > /sys/fs/cgroup/memory/0/tasks; done
for i in `jps | grep NodeManager | awk '{print $1}'`; do echo $i > /sys/fs/cgroup/memory/0/tasks; done

## Config for Crail
sudo mkdir -p /mnt/huge
sudo mount -t hugetlbfs nodev /mnt/huge
sudo chmod a+rw /mnt/huge
mkdir /mnt/huge/cachepath
sudo su
echo 4096 > /proc/sys/vm/nr_hugepages
exit

## For disk instances ##
sudo mkdir -p /mnt
sudo mkdir -p /mnt/sdb
sudo mkdir -p /mnt/sdc
sudo mkdir -p /mnt/sdd
sudo mkdir -p /mnt/sde
sudo mkdir -p /mnt/sdf
sudo mkdir -p /mnt/sdg
sudo mkdir -p /mnt/sdh
sudo mkdir -p /mnt/sdi
sudo mkdir -p /mnt/sdj
sudo mkdir -p /mnt/sdk
sudo mkdir -p /mnt/sdl
sudo mkdir -p /mnt/sdm


sudo umount /mnt/sdm
sudo umount /mnt/sdl
sudo umount /mnt/sdk
sudo umount /mnt/sdj
sudo umount /mnt/sdi
sudo umount /mnt/sdh
sudo umount /mnt/sdg
sudo umount /mnt/sdf
sudo umount /mnt/sde
sudo umount /mnt/sdd
sudo umount /mnt/sdc
sudo umount /mnt/sdb

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

sudo mkfs.ext4 /dev/xvdd
sudo mount /dev/xvdd /mnt/sdd 
sudo chmod a+rw /mnt/sdd
mkdir /mnt/sdd/yarn
mkdir /mnt/sdd/yarn/local-dir
mkdir /mnt/sdd/hdfsdir
mkdir /mnt/sdd/hdfsdir/dfs
mkdir /mnt/sdd/hdfsdir/dfs/data
mkdir /mnt/sdd/hadoop-tmp

sudo mkfs.ext4 /dev/xvde
sudo mount /dev/xvde /mnt/sde
sudo chmod a+rw /mnt/sde
mkdir /mnt/sde/yarn
mkdir /mnt/sde/yarn/local-dir
mkdir /mnt/sde/hdfsdir
mkdir /mnt/sde/hdfsdir/dfs
mkdir /mnt/sde/hdfsdir/dfs/data
mkdir /mnt/sde/hadoop-tmp

sudo mkfs.ext4 /dev/xvdf
sudo mount /dev/xvdf /mnt/sdf
sudo chmod a+rw /mnt/sdf
mkdir /mnt/sdf/yarn
mkdir /mnt/sdf/yarn/local-dir
mkdir /mnt/sdf/hdfsdir
mkdir /mnt/sdf/hdfsdir/dfs
mkdir /mnt/sdf/hdfsdir/dfs/data
mkdir /mnt/sdf/hadoop-tmp

sudo mkfs.ext4 /dev/xvdg
sudo mount /dev/xvdg /mnt/sdg
sudo chmod a+rw /mnt/sdg
mkdir /mnt/sdg/yarn
mkdir /mnt/sdg/yarn/local-dir
mkdir /mnt/sdg/hdfsdir
mkdir /mnt/sdg/hdfsdir/dfs
mkdir /mnt/sdg/hdfsdir/dfs/data
mkdir /mnt/sdg/hadoop-tmp

sudo mkfs.ext4 /dev/xvdh
sudo mount /dev/xvdh /mnt/sdh 
sudo chmod a+rw /mnt/sdh
mkdir /mnt/sdh/yarn
mkdir /mnt/sdh/yarn/local-dir
mkdir /mnt/sdh/hdfsdir
mkdir /mnt/sdh/hdfsdir/dfs
mkdir /mnt/sdh/hdfsdir/dfs/data
mkdir /mnt/sdh/hadoop-tmp

sudo mkfs.ext4 /dev/xvdi
sudo mount /dev/xvdi /mnt/sdi
sudo chmod a+rw /mnt/sdi
mkdir /mnt/sdi/yarn
mkdir /mnt/sdi/yarn/local-dir
mkdir /mnt/sdi/hdfsdir
mkdir /mnt/sdi/hdfsdir/dfs
mkdir /mnt/sdi/hdfsdir/dfs/data
mkdir /mnt/sdi/hadoop-tmp

sudo mkfs.ext4 /dev/xvdj
sudo mount /dev/xvdj /mnt/sdj 
sudo chmod a+rw /mnt/sdj
mkdir /mnt/sdj/yarn
mkdir /mnt/sdj/yarn/local-dir
mkdir /mnt/sdj/hdfsdir
mkdir /mnt/sdj/hdfsdir/dfs
mkdir /mnt/sdj/hdfsdir/dfs/data
mkdir /mnt/sdj/hadoop-tmp

sudo mkfs.ext4 /dev/xvdk
sudo mount /dev/xvdk /mnt/sdk
sudo chmod a+rw /mnt/sdk
mkdir /mnt/sdk/yarn
mkdir /mnt/sdk/yarn/local-dir
mkdir /mnt/sdk/hdfsdir
mkdir /mnt/sdk/hdfsdir/dfs
mkdir /mnt/sdk/hdfsdir/dfs/data
mkdir /mnt/sdk/hadoop-tmp

sudo mkfs.ext4 /dev/xvdl
sudo mount /dev/xvdl /mnt/sdl
sudo chmod a+rw /mnt/sdl
mkdir /mnt/sdl/yarn
mkdir /mnt/sdl/yarn/local-dir
mkdir /mnt/sdl/hdfsdir
mkdir /mnt/sdl/hdfsdir/dfs
mkdir /mnt/sdl/hdfsdir/dfs/data
mkdir /mnt/sdl/hadoop-tmp

sudo mkfs.ext4 /dev/xvdm
sudo mount /dev/xvdm /mnt/sdm
sudo chmod a+rw /mnt/sdm
mkdir /mnt/sdm/yarn
mkdir /mnt/sdm/yarn/local-dir
mkdir /mnt/sdm/hdfsdir
mkdir /mnt/sdm/hdfsdir/dfs
mkdir /mnt/sdm/hdfsdir/dfs/data
mkdir /mnt/sdm/hadoop-tmp

####### Launch Hadoop ##########
## https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html
## the first time, the namenode must be formatted

#	/usr/local/hadoop/bin/hdfs namenode -format 

## if your slaves file is set and SSH enabled to all nodes, then can launch all with the following

#	/usr/local/hadoop/sbin/start-dfs.sh

## start YARN across cluster

#	/usr/local/hadoop/sbin/start-yarn.sh

