#!/bin/bash

cd /home/ftp_get/

# number of list file's lines 
LINENUM=`awk 'END{print NR}' "$PWD"/list.txt`   

get_file(){
 
    lftp mds:mds@192.168.1.10:21 << EEEEOF
    cd "$DIRNAME"
    get "$BASENAME"
    bye
EEEEOF
    if [ $? != 0 ]
    then
       echo "++ Get file: $WILLGET from ftp server failed ++`date +%D-%T`" >> yftp_cmp.log
	let N+=1
       continue 
    fi
    GET_MD5=`md5sum  "$BASENAME" | awk '{print $1}'`
    GET_SHA=`sha1sum  "$BASENAME" | awk '{print $1}'`
}


get_nth_queue(){
WILLGET=`sed -n ''$N'p' list.txt | awk -F^ '{print $1}' `
S_MD5=`sed -n ''$N'p' list.txt | awk '{print $(NF-1)}'`
S_SHA=`sed -n ''$N'p' list.txt | awk '{print $NF}'`
DIRNAME=`dirname "$WILLGET"`
BASENAME=`basename "$WILLGET"`    
}
                                                                                                                                         
     
# compare the MD5 & SHA1  
N=1
while [ $N -le $LINENUM ]
do

    get_nth_queue
    get_file	
    if [ $S_MD5 = $GET_MD5 ] && [ $GET_SHA = $S_SHA ];then
	rm -rf $BASENAME
        let N+=1
	continue 	
    fi
    echo  == File:$WILLGET from ftp server was bad ==`date +%D-%T` >> yftp_cmp.log
    let N+=1

done



