# sdfs

Simple Distributed File System

Dependencies:
===========================================================
    yum install -y epel-release \
    cmake libtool automake gcc gcc-c++ redhat-lsb \
    libuuid-devel libaio-devel flex bison python2-futurist \
    jemalloc-devel libtirpc-devel libattr libattr-devel \
    etcd yajl-devel curl-devel redis hiredis-devel \
    python-paramiko redhat-lsb expect gperftools \
    sqlite-devel libattr libattr-devel fuse-devel \
    openssl-devel rpcbind

    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    pip install python-etcd futurist
    # or:
    # wget http://www.dnspython.org/kits/1.16.0/dnspython-1.16.0.tar.gz
    # tar -xzvf dnspython-1.16.0.tar.gz
    # cd dnspython-1.16.0
    # python setup.py install
    # wget https://files.pythonhosted.org/packages/a1/da/616a4d073642da5dd432e5289b7c1cb0963cc5dde23d1ecb8d726821ab41/python-etcd-0.4.5.tar.gz
    # tar -xzvf python-etcd-0.4.5.tar.gz
    # cd python-etcd-0.4.5
    # python setup.py install


    yum install -y yasm
    git clone https://github.com/01org/isa-l.git
    cd isa-l
    ./autogen.sh 
    ./configure 
    make install

Installation
===========================================================
    cd ${SRC_DIR}
    mkdir build
    cd build
    cmake ..
    make
    sudo make install

Configuration
===========================================================
    1./*prepare disk, from 0 to num_of_your_disks for each node*/
    mkdir -p /opt/sdfs/data/cds/0
    mkfs.ext4 /dev/sdx
    blkid /dev/sdx
    echo 'UUID="you-disk-uuid" /opt/sdfs/data/cds/0 ext4 user_xattr,noatime,defaults 0 0' >> /etc/fstab
    mount /dev/sdx /opt/sdfs/data/cds/0
    /*prepare a ssd or nvme for redis mount*/
    mkdir -p /opt/sdfs/data/redis/0
    mkfs.ext4 /dev/sdy
    blkid /dev/sdy
    echo 'UUID="you-disk-uuid" /opt/sdfs/data/redis/0 ext4 user_xattr,noatime,defaults 0 0' >> /etc/fstab
    mount /dev/sdy /opt/sdfs/data/redis/0

    2.modify config, only modify one of your nodes:
    vim /opt/sdfs/etc/cluster.conf

    update hosts in first column, cds with num_of_disks for example:

    auto1.host155.vmnode31  redis[0,1] mond[0] cds[0,1,2,3,4,5,6] nfs[0]
    auto1.host155.vmnode32  redis[0,1] mond[0] cds[0,1,2,3,4,5,6] nfs[0]
    auto1.host155.vmnode33  redis[0,1] mond[0] cds[0,1,2,3,4,5,6] nfs[0]

    vim /opt/sdfs/etc/sdfs.conf 

    update gloconf.networks, if only single host,then add config:solomode on; for example:

    networks {
        192.168.140.0/8;
    }

    vim /etc/hosts

    update hosts, for example:

    192.168.140.31 auto1.host155.vmnode31
    192.168.140.32 auto1.host155.vmnode32
    192.168.140.33 auto1.host155.vmnode33


Create
===========================================================

    /opt/sdfs/app/admin/cluster.py sshkey --hosts auto1.host155.vmnode31,auto1.host155.vmnode32,auto1.host155.vmnode33
    /opt/sdfs/app/admin/cluster.py create --hosts auto1.host155.vmnode31,auto1.host155.vmnode32,auto1.host155.vmnode33

Usage
===========================================================

    sdfs --help

Auto Testing
===========================================================
    cd test
    test.py

FAQ
===========================================================
   1.Drectories can only be mounted by nfs or cifs after the share has been created
   2.Using nfs3 protocol, exec the mount command likes: mount -t nfs -o vers=3 192.168.140.31:/test /mnt/test
