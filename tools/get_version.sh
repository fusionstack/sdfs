#!/bin/bash

FUNC_FILE=$1/include/get_version.h
FUNC_BASE=$1/tools/get_version.base

rm -f $FUNC_FILE
cat $FUNC_BASE | sed -n '1,5p' >> $FUNC_FILE

echo "#define YVERSION \\" >> $FUNC_FILE


#echo "\"Version:     `cat ../v_num|cut -d '_' -f 1` \\">> $FUNC_FILE
#echo "\nInternalID:  `cat ../v_num|cut -d '_' -f 2` \\">> $FUNC_FILE
#echo $FUNC_FILE
echo "\"BuildId:     `git show |grep -i ^commit |awk '{print $2}'` \\" >> $FUNC_FILE
echo "\nDate:        `git show |grep ^Date:|cut -c 9-`    \\" >> $FUNC_FILE
echo "\nBranch:      SDFS/`git branch |grep '^\*' |awk '{print $2}'`    \\" >> $FUNC_FILE
echo "\nSystem:      `lsb_release -i | awk '{print $3}'` `uname -ir` \\" >> $FUNC_FILE
echo "\nGlibc:       `/lib64/libc.so.6 | head -n 1` \"" >> $FUNC_FILE


echo >> $FUNC_FILE
cat $FUNC_BASE | sed -n '6,12p'>> $FUNC_FILE

