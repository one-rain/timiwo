#!/bin/bash

DIR_ROOT=$(cd `dirname $0`; cd ..; pwd)
source ${DIR_ROOT}/bin/common.sh

cat ${DIR_ROOT}/config/test.json | jq -c -r ".task[]" | while read row
do
    echo ${row} | jq '.table' | xargs
    #_jq() {
    #    echo ${row} | jq -r ${1}
    #}
    #echo $(_jq '.status')
done