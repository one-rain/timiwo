#!/usr/bin/python3
# coding: utf-8
# author: wangrun

import os
import sys
import json
import time
import util

'''
    解析数据仓库下的所有文件，生成任务json
'''

dir_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def get_databases():
    dir_db  = ['ads', 'dwd', 'dws', 'ods']
    databases = []
    for db in dir_db:
        databases.append(get_databases_hive(db))
    return databases

def get_databases_hive(name):
    t1 = get_tables(dir_root + "/warehouse/hive/" + name, ".hql", [])
    return {'name': name, 'type': 'hive', 'tables': t1}

def get_tables(p, suffixs, tables):
    items = os.listdir(p)
    sep = os.path.sep
    for item in items:
        obj = os.path.join(p, item)
        if os.path.isdir(obj):
            get_tables(obj, suffixs, tables)
        elif os.path.isfile(obj):
            f = os.path.splitext(obj)
            name = f[0].split(sep)[-1]
            suffix = f[-1]
            if suffix in suffixs:
                table = {'name': name, 'suffix': suffix, 'path': obj}
                tables.append(table)
        else:
            print("%s is not dir or file." % obj)
    return tables

def read_job(job_id):
    jf = dir_root + '/jobs/' + job_id + '.json'
    with open(jf, 'r', encoding='utf8') as f:
        return json.load(f)

# 更新json中表的执行结果
def update_result(job_id, database, table, result):
    s = read_job(job_id)
    code = 0
    databases = s['databases']
    for i, d in enumerate(databases):
        if d['name'] == database:
            tables = d['tables']
            for j, t in enumerate(tables):
                if t['name'] == table:
                    s['databases'][i]['tables'][j]['result'] = result
                    s['databases'][i]['tables'][j]['end_time'] = int(time.time() * 1000)
                    code = 1
                    break

            if code == 1:
                break
    
    if code == 0:
        print("update result failed. not found table %s in database %s." %(table, database))

'''
    解析hql文件，获取表依赖关系
'''
def get_dependents(databases):
    dependents = {}
    for db in databases:
        dependents.update(get_table_dependents(db))

    return dependents

def get_table_dependents(db):
    dependents = {}
    db_name = db['name']
    if db['type'] != 'hive':
        print("%s is not hive database." %(db_name))
        return None
    
    for tb in db['tables']:
        tb_name = db_name + "." + tb['name']
        dependents[tb_name] = []
        for dep in util.parse_dependents(tb['path']):
            if dep != tb_name and dep != tb['name']:
                dependents[tb_name].append(dep)
    return dependents

def create_job(tag):
    now_time = time.strftime('%Y%m%d%H%M%S', time.localtime())
    job_id = now_time + '_' + tag
    dag_id = job_id + '_dag'
    task_id = job_id + '_task'
    day = '2021-02-25'

    databases = get_databases()
    dependents = get_dependents(databases)
    dag = util.dependents_to_dag(dependents)
    flow = util.dag_sort(dag)
    task_info = []
    for node in flow:
        task_info.append({'table': node, 'status': 0, 'start_time': 0, 'end_time': 0})

    tables = {'databases': databases}
    dag = {'job_id': job_id, 'dependent': dependents, 'dag': dag}
    task = {'job_id': job_id, 'status': 0, 'start_time': 0, 'end_time': 0, 'date': day, 'type': 'day', 'task': task_info}

    with open(dir_root + '/config/tables.json', 'w') as f:
        json.dump(tables, f, indent = 2, sort_keys = True, ensure_ascii = False)

    with open(dir_root + '/config/' + dag_id + '.json', 'w') as f:
        json.dump(dag, f, indent = 2, sort_keys = True, ensure_ascii = False)

    with open(dir_root + '/config/' + task_id + '.json', 'w') as f:
        json.dump(task, f, indent = 2, sort_keys = True, ensure_ascii = False)
    
    return job_id


if __name__ == '__main__':
    if len(sys.argv) > 1:
        args = sys.argv
        create_job(args[1])
    else:
        create_job('daily')