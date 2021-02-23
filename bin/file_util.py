# coding: utf-8
# author: wangrun

def parse_hql(file):
    tables = set()
    with open(file, 'r', encoding='utf8') as sql_file:
        s = sql_file.read().strip().lower().replace('\n', ' ').replace('\r', ' ').replace('\t', '    ')
        parts = s.split(' ')
        flag = False
        for v in parts:
            val = v.strip()
            if flag and not val == '' and not val.startswith('(') and not val.startswith('select '):
                tables.add(val)

            if val == 'from' or val == 'join':
                flag = True
            elif flag and val == '':
                flag = True
            else:
                flag = False
    return tables