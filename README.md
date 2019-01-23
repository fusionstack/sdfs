# sdfs
Simple Distributed File System
===========================================================
Dependencies:
===========================================================
    epel-release \ 
    cmake libtool automake gcc gcc-g++ redhat-lsb \
    libuuid-devel libaio-devel flex bison python2-futurist \
    jemalloc-devel libtirpc-devel libattr libattr-devel \
    etcd yajl-devel curl-devel redis hiredis-devel \
    python-paramiko redhat-lsb expect gperftools \
    sqlite-devel libattr libattr-devel fuse-devel \
    openssl-devel rpcbind

    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    pip install python-etcd futurist


    yum install -y yasm
    git clone https://github.com/01org/isa-l.git
    cd isa-l
    ./autogen.sh 
    ./configure 
    make install
===========================================================
Installation
===========================================================
    cd ${SRC_DIR}
    mkdir build
    cd build
    cmake ..
    make
    sudo make install

===========================================================
Configuration
===========================================================
vim /opt/sdfs/etc/cluster.conf

update hosts in first column, for example:

auto1.host155.vmnode31  redis[0,1] mond[0] cds[0,1,2,3,4,5,6] nfs[0]
auto1.host155.vmnode32  redis[0,1] mond[0] cds[0,1,2,3,4,5,6] nfs[0]
auto1.host155.vmnode33  redis[0,1] mond[0] cds[0,1,2,3,4,5,6] nfs[0]

vim /opt/sdfs/etc/sdfs.conf 

update gloconf.networks , for example:

networks {
        192.168.140.0/8;
}

vim /etc/hosts

update hosts, for example:

192.168.140.31 auto1.host155.vmnode31
192.168.140.32 auto1.host155.vmnode32
192.168.140.33 auto1.host155.vmnode33


===========================================================
Create
===========================================================

/opt/sdfs/app/admin/cluster.py create --hosts auto1.host155.vmnode31,auto1.host155.vmnode32,auto1.host155.vmnode33

===========================================================
Auto Testing
===========================================================
    cd test
    test.py

===========================================================
