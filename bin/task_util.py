#!/usr/bin/python3
# coding: utf-8
# author: wangrun

import os
import sys
import json
import time
from collections import Counter
from collections import deque
from itertools import chain

dir_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def get_databases():
    dir_db  = ['ads', 'dwd', 'dws', 'ods']
    databases = []
    for db in dir_db:
        databases.append(get_databases_hive(db))
    return databases

def get_databases_hive(name):
    table = get_tables(dir_root + "/warehouse/hive/" + name, ".hql", [])
    return {'name': name, 'type': 'hive', 'tables': table}

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

# 更新json中表的执行结果
def update_table_status(job_id, table, result):
    f = dir_root + '/config/' + job_id + '_task.json'
    flag = False
    new_task = {}
    with open(f, 'r') as data:
        j = json.load(data)
        for i in range(len(j['task'])):
            if j['task'][i]['table'] == table:
                flag = True
                j['task'][i]['status'] = result
                if result == 1:
                    j['task'][i]['start_time'] = int(time.time() * 1000)
                else:
                    j['task'][i]['end_time'] = int(time.time() * 1000)
                
                break
        
        if flag and j['start_time'] == 0:
            j['start_time'] = int(time.time() * 1000)
            j['status'] = 1
            new_task = j
        elif flag:
            new_task = j
    data.close()

    if flag:
        with open(f, 'w') as file:
            json.dump(new_task, file, indent = 2, sort_keys = True, ensure_ascii = False)
        file.close()
        return 0
    else:
        #print('update table not match table %s, job id: %s' % (table, job_id))
        return 2
        

# 更新总任务执行结果
def update_task_status(job_id, result):
    f = dir_root + '/config/' + job_id + '_task.json'
    new_task = None
    with open(f, 'rb') as data:
        j = json.load(data)
        j['end_time'] = int(time.time() * 1000)
        j['status'] = result
        new_task = j

    data.close()

    if not new_task:
        with open(f, 'w') as file:
            json.dump(new_task, file, indent = 2, sort_keys = True, ensure_ascii = False)
        file.close()
    else:
        print('update task not match job id %s' % (job_id))


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
        for dep in parse_dependents(tb['path']):
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
    dag = dependents_to_dag(dependents)
    flow = dag_sort(dag)
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

# 解析HQL，获取依赖的表名，结果为: {'database.table', ...}
def parse_dependents(file):
    tables = set()
    with open(file, 'r') as sql_file:
        s = sql_file.read().strip().lower().replace('\n', ' ').replace('\r', ' ').replace('\t', '    ')
        parts = s.split(' ')
        flag = False
        for v in parts:
            val = v.strip()
            if flag and not val == '' and not val.startswith('(') and not val.startswith('select ') and not val.startswith('alias_with_'):
                tables.add(val.replace(')', '').replace(';', ''))

            if val == 'from' or val == 'join':
                flag = True
            elif flag and val == '':
                flag = True
            else:
                flag = False
    sql_file.close()
    return tables

'''
    根据依赖关系，生成dag
    graph结构： {'table1': ['table5', 'table6'], ...}
'''
def dag_sort(graph):
    cnt = Counter()
    for val in list(chain.from_iterable(graph.values())):
        cnt[val] += 1

    degree = {node: cnt[node] for node in graph}
    #print(degree)

    # 将所有入度为0的顶点入列
    queue = deque()
    zero_indeg = [node for node in graph if degree[node] == 0]
    queue.extend(zero_indeg)
    
    # 拓扑排序
    top_order = list()
    while len(queue):
        node = queue.pop()
        top_order.append(node)
        for adj in graph[node]: # 从图中拿走这一点，就是把它的邻接点的入度-1
            degree[adj] -= 1
            if degree[adj] == 0: # 上一步操作之后，还要对图中的结点进行入度判断
                queue.append(adj)

    if len(top_order) != len(graph): # 最后结果不包含全部的点，则图不连通
        print('create dag failed. the dag size not eq graph.')
        return None
    else:
        return top_order

'''
    流向关系数据结构，它是依赖关系数据结构的逆向
    dependents: 表依赖关系结构，例如：{'table1': ['table5', 'table6'], ...}
'''
def dependents_to_dag(dependents):
    graph = {}
    for key, val in dependents.items():
        if len(val) > 0:
            for v in val:
                if v not in graph:
                    graph[v] = [key]
                else:
                    graph[v].append(key)
            graph[key] = []
        elif key not in graph:
            graph[key] = []
    return graph

sf = []
sq = set()

'''
    根据流向关系图结构，查找从某个节点开始的正向流向
'''
def dag_cut_down(graph, node):
    sub = graph[node]
    size = len(sub)
    if size < 1:
        return node
    else:
        for sn in sub:
            if sn not in sq:
                sf.append(sn)
                sq.add(sn)
        
        for sn in sub:
            dag_cut_down(graph, sn)

'''
    根据流向关系图结构，查找从某个节点开始的逆向流向
'''
def dag_cut_up(graph, node):
    ps = []
    for key in graph:
        for n in graph[key]:
            if n == node:
                ps.append(key)
    print(ps)
    if len(ps) == 0:
        graph.pop(node)
        return node
    else:
        for p in ps:
            sf.append(dag_cut_up(graph, p))



def dag_cut(graph, node, tag):
    if tag > 0:
        sf.append(node)
        dag_cut_down(graph, node)
        return sf
    elif tag < 0:
        dag_cut_up(graph, node)
    else:
        return sf

def test():
    graph = {
        'table1': ['table5', 'table6'],
        'table2': ['table4', 'table6'],
        'table3': ['table5', 'table7'],
        'table4': ['table10', 'table11', 'table12'],
        'table5': ['table7', 'table8'],
        'table6': ['table10'],
        'table7': ['table11', 'table12'],
        'table8': ['table10', 'table12'],
        'table9': ['table11'],
        'table10': [],
        'table11': [],
        'table12': [],
        'table13': []
    }
    #print(dag_sort(graph))
    #dag_cut_down(graph, 'table2')
    dag_cut_up(graph, 'table10')
    print(sf)

test()
#update_table_status('20210226190738_daily', 'dwd.dwd_user', 1)