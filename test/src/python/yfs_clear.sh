YFS=/sysy/yfs

cd $YFS/app/sbin

python yfs-daemon.py -k all

rm -rf $YFS/cds/1/*
rm -rf $YFS/cds/2/*
rm -rf $YFS/cds/3/*
rm -rf $YFS/mds/*

rm -rf /var/log/yfs*.log
rm -rf /var/log/ynfs.log

python yfs-daemon.py -s mds
python yfs-daemon.py -s ynfs
# python yfs-daemon.py -s cds

sleep 1
