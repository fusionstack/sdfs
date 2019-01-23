#!/bin/bash

SBIN_DIR="/sysy/yfs/app/sbin"
BIN_DIR="/sysy/yfs/app/bin"
LIB_DIR="/sysy/yfs/app/lib/python"

YFS_SHELL="yfs-shell.py"
START_TPL="local_startup.tpl"
SH_SCRIPTS="gen_sshkey.sh"
GEN_SCRIPTS="gen_data.py"
OUTPUT_LIB="color_output.py"
FENCE="fence_test.py"
TAILLOG="taillog.sh"

usage()
{
    echo "Usage: $0 [OPTIONS]"
    echo "-i install"
    echo "-e remove"

    exit 1
}

install()
{
    mkdir -p $SBIN_DIR

    mkdir -p $BIN_DIR

    mkdir -p $LIB_DIR

    chmod +x $YFS_SHELL
    cp $YFS_SHELL $SBIN_DIR

    cp $START_TPL $SBIN_DIR
    
    cp $SH_SCRIPTS $BIN_DIR

    chmod +x $GEN_SCRIPTS
    cp $GEN_SCRIPTS $BIN_DIR

    cp $OUTPUT_LIB $LIB_DIR

    chmod +x $FENCE
    cp $FENCE $BIN_DIR

    chmod +x $TAILLOG
    cp $TAILLOG $BIN_DIR
}
    

remove()
{
    rm -fr $SBIN_DIR/$START_SCRIPTS

    rm -fr $LIB_DIR/$OUTPUT_LIB
}

if [ $# -lt 1 ]
then 
    usage
fi

while getopts ieh options
do
    case $options in
        i)
        echo "install scripts"
        install
        ;;
        e)
        echo "remove scripts"
        remove
        ;; 
        h)
        usage
        ;;
        \?)
        usage
        ;;
    esac
done
        
