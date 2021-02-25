# coding: utf-8
# author: wangrun

from collections import Counter
from collections import deque
from itertools import chain

# 解析HQL，获取依赖的表名，结果为: {'database.table', ...}
def parse_dependents(file):
    tables = set()
    with open(file, 'r', encoding='utf8') as sql_file:
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

'''
    根据流向关系图结构，查找从某个节点开始的dag
'''
def dag_cut(graph, node):
    print('')

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
    #g = {'dws.dws_active': [], 'ads.ads_basis_collect': ['ads.ads_active_month'], 'ads.ads_active_month': [], 'dwd.dwd_store': ['dws.dws_view_product'], 'ods.ods_log_app': ['dwd.dwd_app'], 'dwd.dwd_app': ['dws.dws_active'], 'dwd.dwd_product': ['dws.dws_view_product'], 'dwd.dwd_order': ['dws.dws_user_order'], 'dwd.dwd_user': ['dws.dws_user_order'], 'dws.dws_user_order': [], 'dws.dws_view_product': []}
    #print(dependents_to_dag(graph))
    print(dag_sort(graph))

#test()