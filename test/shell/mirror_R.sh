#!/bin/bash

TARGET="/home/gj/linux"
MYUSER="mds"
MYPASSWD="mds"

do_it()
{
SERVERPATH=`date +%s`

        lftp $MYUSER:$MYPASSWD@192.168.1.119:21 << EEEEOF
        mkdir "$SERVERPATH"
        cd "$SERVERPATH"
        lcd "$TARGET"
        lcd "../"
        mirror -R $TARGET
        bye
EEEEOF

        sleep 2

        if [ $? != 0 ]
        then
                echo "+Mirror $TARGET to $SERVERPATH@192.168.1.10 faile +`date +%D-%T`" >> mirror.log
                continue
        fi
}



while [ 1 != 0 ]
do
        do_it
done
