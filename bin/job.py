
# job.py
# coding: utf-8
# author: wangrun

import os
import json

def get_job():
    databases = get_databases()
    job = {'id': 12, 'type': 'day', 'date': '2021-01-12', 'hour': 0, 'databases': databases}
    return job
    #return json.dumps(job)

def get_databases():
    databases = []
    t1 = get_tables("F:\workspace\hamster\warehouse\hive\dws", ".hql")
    dws = {'name': 'dws', 'type': 'hive', 'tables': t1}
    databases.append(dws)

    t2 = get_tables("F:\workspace\hamster\warehouse\hive\ods", ".hql")
    ods = {'name': 'ods', 'type': 'hive', 'tables': t2}
    databases.append(ods)
    return databases

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
                table = {'name': name, 'suffix': suffix, 'path': obj, 'result': 0}
                tables.append(table)
        else:
            print("%s is not dir or file." % obj)
    return tables

def update_result(database, table, result):
    s = get_job()
    code = 0
    databases = s['databases']
    for i, d in enumerate(databases):
        if d['name'] == database:
            tables = d['tables']
            for j, t in enumerate(tables):
                if t['name'] == table:
                    s['databases'][i]['tables'][j]['result'] = result
                    code = 1
                    break
        
            if code == 1:
                break
    
    if code == 0:
        print("update result failed. not found %s in %s." %(table, database))
    print(s)
    

update_result('dws', 'c', 1)




#with open('test.json', 'w') as f:
#    f.write(s)

#with open('test.json', 'r') as f:
#    d = f.read()
#    print(json.loads(d))