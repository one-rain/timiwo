#!/bin/bash

#########################################
#  常用方法公用脚本                        #
#  Author: wangrun                      #
#  Date:  2018-05-25                    #
#########################################

PROJECT_ROOT=$(cd $(dirname ${BASH_SOURCE[0]}); cd ..; pwd)
OSS_PATH=""
source /etc/profile

function info() {
    echo "`date +"%Y-%m-%d %H:%M:%S"` Info: $1"
}

function warn() {
    echo "`date +"%Y-%m-%d %H:%M:%S"` Warn: $1"
}

function error() {
    echo "`date +"%Y-%m-%d %H:%M:%S"` Error: $1"
}

function send_mail() {
    if [ $# -eq 2 ]; then
        #内网
        info "send email."
        curl -H "content-type: application/x-www-form-urlencoded; charset=UTF-8" -d "content=${1}" "http://172.31.2.189/api/notice/common/alarm?tag=${2}&valid=1"
    else
        error "参数错误，第一个参数为邮件内容，第二个为报警标识"
        exit 2
    fi
}

function get_ip_inter() {
    LOCAL_IP_INTER=$(/sbin/ifconfig -a | grep inet | grep -v 127.0.0.1 | grep -v inet6 | awk '{print $2}' | tr -d 'addr:' | awk 'NR==1')
    info "inter ip ${LOCAL_IP_INTER}"
}

function upload_oss() {
    if [ ! -f "${1}" ]; then
        echo ""
        info "${1} 文件不存在，请检查后再操作"
        if [ $# -eq 3 ]; then
            if [ "${LOCAL_IP_INTER}" == "" ]; then
                get_ip_inter
            fi
            send_mail "内网 ${LOCAL_IP_INTER} 上传文件 ${1} 不存在." "${3}" 
        fi
        exit 2
    elif [ $# -eq 2 -o $# -eq 3 ]; then
        /usr/sbin/ossutil cp ${1} ${2} --config-file /home/hadoop/config/oss-put.config
        if [ $? -eq 0 ]; then
            info "success upload ${1} to ${2}."
        else
            error "upload ${1} to ${2} failed."
            if [ $# -eq 3 ]; then
                if [ "${LOCAL_IP_INTER}" == "" ]; then
                    get_ip_inter
                fi
                send_mail "内网 ${LOCAL_IP_INTER} 上传 ${1} 到 ${2} 失败." "${3}" 
            fi
            exit 2
        fi
    else
        info "参数错误，参数为：源文件（必须） 目标存储路径（必须） 报警标签（选填）"
    fi
}

function download_oss() {
    if [ $# -eq 2 -o $# -eq 3 ]; then
        ossutil cp --update ${1} ${2} --config-file /home/hadoop/config/oss-get.config
        if [ $? -eq 0 ]; then
            info "success download ${1} to ${2}."
        else
            error "download ${1} to ${2} failed."
            if [ $# -eq 3 ]; then
                send_mail "下载${1} 到 ${2} 失败." "${3}"
            fi
        fi
    else
        info "Use 2 parameters. 1: source file, 2: target path"
        exit 3
    fi
}
