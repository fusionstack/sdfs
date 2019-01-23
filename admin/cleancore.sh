#!/bin/bash


if [ $# != 1 ] ; then
        echo "Usage:"
        echo "./cleancore.sh <path>"
        echo ""
        exit 1
fi

CORE_PATH=$1
HOME="/opt/sdfs"
KEEP_CORE_FILE_NUM=3
EXPIRE_TIME=$((30*24*60*60))

declare -A dic
declare -a core_date_key

SYSTIME="date +%Y-%m-%d_%H:%M:%S"

echo "`${SYSTIME}` : task beginning ..."
get_file_time()
{
        file=$1
        time1=`stat ${file} | grep -E "Modify|更改"  | awk '{print $2}'`
        time2=`stat ${file} | grep -E "Modify|更改"  | awk '{print $3}' | cut -d"." -f1`
        systime=`date -d "${time1} ${time2}" +%s`
        echo "${systime}"
}

get_core_file_sbin()
{
        file=$1

        if [ ${file:0:4} == "uss_" ]; then
                echo "${HOME}/app/sbin/${file}"
        else
                echo "${HOME}/app/bin/${file}"
        fi
}

delete_core()
{
        core_sbin=$1
        abs_core_path=$2

        gdb ${core_sbin} ${abs_core_path} --eval-command="p ret" --eval-command="bt"  --eval-command="thread apply all bt" --eval-command="quit" 2>&1
        rm -f ${abs_core_path}
}

if [ ! -d "${CORE_PATH}" ]; then
        echo "`${SYSTIME}` : ERROR:($1) NO SUCH FILE OR DIRECTORY"
        exit 2
fi

#begin to clear core files
cd ${CORE_PATH}
echo ""
echo "`${SYSTIME}` : >>>>>>>>>>>>>>>>>begin to clean core file, path(${CORE_PATH})<<<<<<<<<<<<<<<<<<<<<"
for filename in `ls | awk -F'-' '{print $2}' | uniq`; do
        unset dic[*]
        core_file_sbin=`get_core_file_sbin ${filename}`
        core_files="core-"$filename"-*"
        now=`date +%s`
        for core_file in `ls ${core_files}`; do
                core_file_date=`get_file_time ${core_file}`
                interval=$((${now} - ${core_file_date}))
                #delete expired core
                if [ ${interval} -gt ${EXPIRE_TIME} ]; then
                        echo "`${SYSTIME}` : ${core_file} too old, time:`date -d @${core_file_date}`, next will remove it..."
                        delete_core ${core_file_sbin} ${CORE_PATH}/${core_file}
                        echo "**************************************************************************************************************"
                        continue
                fi

                tmp_file=`echo ${dic["${core_file_date}"]}`
                #delete the same time core
                if [ ${#tmp_file} -gt 0 ]; then
                        echo "core_file:${core_file} time is the same as ${dic["${core_file_date}"]}, will remove ${core_file}"
                        delete_core ${core_file_sbin} ${CORE_PATH}/${core_file}
                        echo "**************************************************************************************************************"
                        continue
                fi

                dic["${core_file_date}"]=${core_file}
        done

        #keep one core in 10 minites
        core_date_key=(${!dic[*]})
        array_size=${#core_date_key[@]}
        for ((i=0; i<${array_size}; i++)); do
                if [ "${core_date_key[$i]}" == "" ]; then
                        continue
                fi
                for ((j=$i+1; j<${array_size}; j++)); do
                        if [ "${core_date_key[$j]}" == "" ]; then
                                continue
                        fi

                        interval=$((${core_date_key[$j]} - ${core_date_key[$i]}))
                        if [ ${interval#-} -lt 600 ] ; then
                                #delete core file, keep one core between 10 minites
                                remove_core_file=${dic["${core_date_key[$j]}"]}
                                date_key=${core_date_key[$j]}
                                echo "`${SYSTIME}` : ****************remove core file : ${remove_core_file}, date:${date_key}"
                                rm -f ${CORE_PATH}/${remove_core_file}
                                unset core_date_key[$j]
                                unset dic["${date_key}"]

                        fi
                done
        done

        core_date_key=(${!dic[*]})
        array_size=${#core_date_key[@]}
        if [ ${array_size} -gt 3 ]; then
                #sort array core_date_key
                for ((i=0; i<${array_size}; i++)); do
                        for ((j=$i+1; j<${array_size}; j++)); do
                                if [ ${core_date_key[$j]} -gt ${core_date_key[$i]} ]; then
                                        tmp_value=${core_date_key[$i]}
                                        core_date_key[$i]=${core_date_key[$j]}
                                        core_date_key[$j]=${tmp_value}
                                fi
                        done
                done

                #delete old core file, keep max 3 cores
                for ((i=3; i<${array_size}; i++)); do
                        remove_core_file=${dic["${core_date_key[$i]}"]}
                        date_key=${core_date_key[$i]}
                        echo "`${SYSTIME}` : ***************remove ${remove_core_file} , date:`date -d @${date_key}`************"
                        delete_core ${core_file_sbin} ${CORE_PATH}/${core_file}
                        echo "**************************************************************************************************************"
                done
        fi

done
echo "`${SYSTIME}` : clean core file OK !"

echo ""
echo "`${SYSTIME}` : task over ..."
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
exit 0
