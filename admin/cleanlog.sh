#!/bin/bash

#max lich_*.log file size 500M
((MAX_LOGFILE_SIZE=500*1024*1024))
#max redis_*.log file size 10M
((MAX_REDIS_LOG_SIZE=10*1024*1024))
#default backup files number
((NUM_BACKUP_FILES=3))
#default log file backup path, must be absolute path


SYSTIME="date +%Y-%m-%d_%H:%M:%S"
echo "`${SYSTIME}` : task begnning ..."
RUNNING=`ps -ef | grep /cleanlog.sh | grep -v "grep" | wc -l`
if [ "${RUNNING}" -gt 3 ] ; then
        echo ""
        echo "`${SYSTIME}` : WARNNING : cleanlog.sh is already running. next will exit !"
        exit 3
fi

if [ $# != 2 ] ; then
        echo "Usage:"
        echo "./cleanlog.sh <log_path> <log_backup_path>"
        echo ""
        exit 1
fi

LOG_PATH=$1
LOG_BACKUP_PATH=$2

get_file_size()
{
        file=$1;
        size=`ls -l $file | awk '{print $5}'`
        echo "${size}"
}

check_backup_files()
{
        backup_path=$1;
        logfile=$2

        echo "backup_path ${backup_path}"
        echo "logfile ${logfile}"

        if [ ! -d "${backup_path}" ]; then
                echo "`${SYSTIME}` : ERROR:[${backup_path}]NO SUCH FILE OR DIRECTORY";
                return 2;
        fi

        cd ${backup_path};
        #for (( num=`ls $logfile* | wc -l` ; `echo ${num}|bc` > `echo ${NUM_BACKUP_FILES} | bc`; ((num=$num-1)) ))
        num=`ls $logfile* 2>&1 | wc -l`
        if [ "${num}" -gt "${NUM_BACKUP_FILES}" ]; then
        #check backup files, if more than default number, delete the old backup file
                until [ "${num}" -eq "${NUM_BACKUP_FILES}" ]
                do
                        ((old_num=99999999999999))
                        for backup_file in `ls $logfile*`; do
                                tmp_num=`echo ${backup_file} | awk -F'.' '{print $3}'`
                                if [ "${tmp_num}" != "" -a "${tmp_num}" -lt "${old_num}" ]; then
                                        old_num="${tmp_num}"
                                fi
                        done
                        old_backup_file=${logfile}".${old_num}.*";
                        rm -f ${old_backup_file};
                        echo "`${SYSTIME}` : delete backup filename : ${old_backup_file}";
                        num=$((${num}-1))
                done
        fi

        cd - > /dev/null;
        return 0;
}

if [ ! -d "${LOG_PATH}" ] ; then
        echo "`${SYSTIME}` : ERROR:($1)NO SUCH FILE OR DIRECTORY"
        exit 2
fi

if [ ! -d "${LOG_BACKUP_PATH}" ] ; then
        mkdir -p ${LOG_BACKUP_PATH}
        echo "`${SYSTIME}` : create backup ok"
fi

if [ -d "/var/spool/clientmqueue" ] ; then
        rm -rf /var/spool/clientmqueue
        echo "`${SYSTIME}` : delete rubbish files in directory /var/spool/clientmqueue"
fi

clean_file()
{
        log_file=$1
        filename="${log_file##*/}"

        if [[ "${log_file}" =~ "redis_" ]]; then
                max_size=${MAX_REDIS_LOG_SIZE}
        else
                max_size=${MAX_LOGFILE_SIZE}
        fi

        log_file_size=`get_file_size ${log_file}` #get file size, if more than default value, cp to backup and clear it
        echo ""
        echo "`${SYSTIME}` : ******************** (${log_file}) *******************************"
        echo "`${SYSTIME}` : file size ${log_file_size}"

        echo "`${SYSTIME}` : max log file size:${max_size}"
        if [ "${log_file_size}"  -gt "${max_size}" ] ; then
                echo "`${SYSTIME}` : ${log_file} clear begin ..."
                #backup file format: lich_0.log.20141117000000.gz
                backup_file_name=${filename}".`date +%Y%m%d%H%M%S`.gz"
                gzip -c ${log_file} > ${LOG_BACKUP_PATH}/${backup_file_name}
                ret=`echo $?`
                if [ ${ret} -eq 0 ]; then
                        echo "`${SYSTIME}` : new backup file name : ${backup_file_name}"
                        echo "" > ${log_file}
                        echo "`${SYSTIME}` : clear log file(${log_file}) OK !"
                else
                        rm -f ${LOG_BACKUP_PATH}/${backup_file_name} 2>/dev/null
                        echo "`${SYSTIME}` : gzip file(${log_file}) fail !"
                fi
        else
                echo "`${SYSTIME}` : ${log_file} no need to clear !"
        fi

        check_backup_files ${LOG_BACKUP_PATH} ${filename}
        ret=$?
        if [ ${ret} != 0 ]; then
                echo "`${SYSTIME}` : check backup file(${log_file}) ERROR !"
                exit $ret
        fi

        echo "`${SYSTIME}` : check backup file(${log_file}) ok!"
}

scan_dir()
{
        dir=$1

        cd ${dir}
        cur_dir="`pwd`"
        #for log_file in `ls lich_[0-9]*.log`; do
        for file in `ls`; do
                abs_path="${cur_dir}/${file}"
                if [ -d ${abs_path} ]; then
                        if [ "${file}" == "backup" ]; then
                                continue
                        else
                                scan_dir ${abs_path}
                                cd ..
                                cur_dir="`pwd`"
                        fi
                else
                        clean_file ${abs_path}
                fi
        done
}

scan_dir ${LOG_PATH}

echo ""
echo "`${SYSTIME}` : task over ..."
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
exit 0
