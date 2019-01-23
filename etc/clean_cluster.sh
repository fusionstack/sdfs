#!/bin/bash

nodes=`cat /opt/sdfs/etc/cluster.conf |awk '{print $1}'`

for n in $nodes
do
    echo "clean " $n
    ssh $n 'pkill -9 sdfs'
    ssh $n 'rm -rf /opt/sdfs.mond/*/*'
    ssh $n 'rm -rf /opt/sdfs/cds/*/*'
    ssh $n 'rm -rf /opt/sdfs/fuse/*'
    ssh $n 'rm -rf /opt/sdfs/fuse3/*'
    ssh $n 'rm -rf /opt/sdfs/ftp/*'
    ssh $n 'rm -rf /opt/sdfs/nfs/*'
    ssh $n 'rm -rf /opt/sdfs/fsal_sdfs/*'
    ssh $n 'rm -rf /opt/sdfs/etc/cluster.conf'
    ssh $n 'rm -rf /dev/shm/sdfs/*'
    ssh $n 'rm -rf /var/lib/leveldb/*'
done

for n in $nodes
do
    echo "stop zookeeper in " $n
    ssh $n 'systemctl stop zookeeper.service'
done

for n in $nodes
do
    echo "clean zookeeper data in " $n
    ssh $n 'rm -rf /var/lib/zookeeper/version-2'
done

for n in $nodes
do
    echo "start zookeeper in " $n
    ssh $n 'systemctl start zookeeper.service'
done
