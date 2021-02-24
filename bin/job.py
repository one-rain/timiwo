
# job.py
# coding: utf-8
# author: wangrun

'''
    解析数据仓库下的所有文件，生成任务json
'''

import os
import json
import time
import util

dir_root = os.path.abspath('.')

def create_job(id, day):
    databases = []
    databases.append(get_databases_hive('dws'))
    databases.append(get_databases_hive('ods'))

    job = {'id': id, 'type': 'day', 'date': day, 'result': 0, 'databases': databases}

    with open(dir_root + '/jobs/' + id + '.json', 'w', encoding='utf-8') as f:
        json.dump(job, f, indent=2, sort_keys=True, ensure_ascii=False)
    
    return job

def get_databases_hive(name):
    t1 = get_tables(dir_root + "/warehouse/hive/" + name, ".hql")
    return {'name': name, 'type': 'hive', 'tables': t1}

def get_tables(p, suffixs):
    tables = []
    items = os.listdir(p)
    sep = os.path.sep
    for item in items:
        obj = os.path.join(p, item)
        if os.path.isdir(obj):
            get_tables(obj, suffixs)
        elif os.path.isfile(obj):
            f = os.path.splitext(obj)
            name = f[0].split(sep)[-1]
            suffix = f[-1]
            if suffix in suffixs:
                table = {'name': name, 'suffix': suffix, 'path': obj, 'result': 0, 'start_time': int(time.time() * 1000), 'end_time': 0}
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
    1、生成任务json
    2、解析json中的hql文件，获取表依赖关系
    3、生成表依赖关系
'''
def create_dependent():
    now_time = time.strftime('%Y%m%d%H%M%S', time.localtime())
    job_id = "default_" + now_time
    print(job_id)
    #job = create_job(job_id, '2021-02-23')

    job_id = 'default_20210224141503'
    job = read_job(job_id)
    #print(job)

    db = job['databases']
    dependents = {}
    for d in db:
        if d['type'] == 'hive':
            for t in d['tables']:
                table = d['name'] + "." + t['name']
                print(table)
                deps = util.parse_hql(t['path'])
                dependents[table] = []
                for dt in deps:
                    print(dt)
                    if dt != table and dt != t['name']:
                        dependents[table].append(dt)
                
    return dependents

create_dependent()


#update_result('a', 'dws', 'c', 1)


#with open('test.json', 'w') as f:
#    f.write(s)

#with open('test.json', 'r') as f:
#    d = f.read()
#    print(json.loads(d))