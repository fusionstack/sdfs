#!/bin/sh

#set -xv

PLATFORM=`uname -m`

if [ $PLATFORM = 'x86_64' ];then
    CONF_ARGS='--with-pic'
else
    CFLAGS='-D_FILE_OFFSET_BITS=64'
fi

PREFIX='/sysy/yfs/app'

if [ -x ccache ]; then
       CC="ccache gcc $1" CFLAGS="$CFLAGS" ./configure $CONF_ARGS --prefix=$PREFIX
else 
       CC="gcc $1" CFLAGS="$CFLAGS" ./configure $CONF_ARGS --prefix=$PREFIX
fi
