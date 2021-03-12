#!/bin/bash

##################################################################
# 1、 任务调度，                                     #
# 2、 需要临时执行调度的，                                  #
#                                                                 #
#                                                                #
#  Author: wangrun                                               #
#  Date:  2018-05-25                                             #
##################################################################

DIR_ROOT=$(cd `dirname $0`; cd ..; pwd)
source ${DIR_ROOT}/bin/common.sh

function update_statue() {
    job_id="1122"
    table=$2
    status=$3
    cd ${DIR_ROOT}/bin
    result=$(python -c 'import task_util; print task_util.update_table_status("'${job_id}'", "'${table}'", '${status}')')

    if [ "${result}" -eq "0" ]; then
        return 0
    else
        error "update task status failed. job: ${job_id}, table: ${table}, status: ${status}"
        return 2
    fi
}

function start() {
    tag=$1
    cd ${DIR_ROOT}/bin
    #job_id=$(python -c 'import task_util; print task_util.create_job("'${tag}'")')
    job_id="20210226190738_daily"
    info "job id: ${job_id}"

    task_name="${job_id}_task"

    json=$(cat ${DIR_ROOT}/config/tables.json)
    length=$(echo $json | jq '.databases | length')

    # 遍历task.json，根据任务列表顺序执行，根据表名从tables.json中获取
    status=1
    cat ${DIR_ROOT}/config/${task_name}.json | jq -c -r ".task[]" | while read row
    do
        status=`echo ${row} | jq '.status' | xargs`
        table=`echo ${row} | jq '.table' | xargs`
        array=(${table//./ })
        db=${array[0]}
        flag=0
        #info "start execute table: ${table}, now status: ${status}"
        for ((index=0; index<${length}; index++))
        do
            ndb=$(echo $json | jq '.databases['${index}'].name' | xargs)
            if [ "${db}" == "${ndb}" ]; then
                tables=$(echo $json | jq '.databases['${index}'].tables')
                size=$(echo $json | jq '.databases['${index}'].tables | length')
                for ((i=0; i<${size}; i++))
                do
                    tb=$(echo $json | jq '.databases['${index}'].tables['${i}'].name' | xargs)
                    if [ "${ndb}.${tb}" == "${table}" ]; then
                        path=$(echo $json | jq '.databases['${index}'].tables['${i}'].path' | xargs)
                        now_dir=${path%/*}
                        now_file=${path##*/}

                        update_statue ${job_id} ${table} 1

                        cd ${now_dir}
                        info "execute ${tb}, path: ${path}"
                        #hive -hivevar day=${day} -hivevar year=${year} -hivevar month=${month} -hivevar ossPath=${OSS_PATH} -f ./${now_file}
                        if [ $? -eq 0 ]; then
                            flag=3
                        else
                            flag=2
                        fi

                        # update task exeucte result
                        update_statue ${job_id} ${table} ${flag}
                        if [ $? -ne 0 ]; then
                            error "update task result failed. job_id: ${job_id}, table: ${table}"
                        fi
                    fi
                done
            fi
        done

        message=""
        if [ ${flag} -eq 0 ]; then
            message="table ${table} not in tables.json, task execute failed."
        elif [ ${flag} -eq 1 ]; then
            message="${table} is executing."
        elif [ ${flag} -eq 2 ]; then
            status=2
            message="table ${table} task execute failed."
        elif [ ${flag} -eq 3 ]; then
            message="table ${table} task execute success."
        else
            status=2
            message="table ${table} execute result is undefined."
        fi
        info "${message}"
    done

    if [ ${status} -eq 2 ]; then
        message="job ${job_id} execute failed."
    else
        message="job ${job_id} execute success."
    fi
    info "${message}"
}

start 'daily'
