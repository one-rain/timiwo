from collections import Counter
from collections import deque
from itertools import chain

def sort(graph):
    cnt = Counter()
    for val in list(chain.from_iterable(graph.values())):
        cnt[val] += 1

    InDegree = {node: cnt[node] for node in graph}
    print(InDegree)
 
    # 将所有入度为0的顶点入列
    queue = deque()
    zero_indeg = [node for node in graph if InDegree[node] == 0]
    queue.extend(zero_indeg)
    
    # 拓扑排序
    TopOrder = list()
    while len(queue):
        node = queue.popleft()
        TopOrder.append(node)
        for adj in graph[node]: # 从图中拿走这一点，就是把它的邻接点的入度-1
            InDegree[adj] -= 1
            if InDegree[adj] == 0: # 上一步操作之后，还要对图中的结点进行入度判断
                queue.append(adj)
    
    print(TopOrder)

    if len(TopOrder) != len(graph): # 最后结果不包含全部的点，则图不连通
        return False
    else:
        return True

'''
graph = {
    1:[3],
    2:[3,13],
    3:[7],
    4:[5],
    5:[6],
    6:[15],
    7:[10,11,12],
    8:[9],
    9:[10,12],
    10:[14],
    11:[],
    12:[],
    13:[],
    14:[],
    15:[],
}
'''
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
    'table12': []
}

sort(graph)