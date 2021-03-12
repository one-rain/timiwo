#!/usr/bin/python3
# coding: utf-8
# author: wangrun

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                yield from extract_from_part(item)
                #for x in extract_from_part(item):
                #    yield x
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                #print(type(identifier), identifier.ttype, identifier.value, identifier.get_real_name(), identifier.get_name())
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            #print(type(identifier), identifier.ttype, identifier.value, identifier.get_real_name())
            yield item.value.split(' ')[0]
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value

def extract_tables(sql):
    file_parse = sqlparse.parse(sql)[0]
    print(file_parse)
    stream = extract_from_part(file_parse)
    return list(extract_table_identifiers(stream))

if __name__ == '__main__':
    sql1 = """
        select K.a,K.b from (select H.b from (select G.c from (select F.d from
        (select E.e from d1.A, B, d2.C as c, D, E), F), G), H), I, J, K order by 1,2;
    """

    sql2 = """
        CREATE TABLE TABLE_TO_CREATE NOLOGGING AS
        SELECT  DISTINCT
            A.COLA,
            B.COLB,
            DECODE(A.DECODE_CONDITION, 1, '是', '否') DECODED,
            ROW_NUMBER() OVER(PARTITION BY A.CLASS_CONDITION ORDER BY A.RAND_CONDITION DESC) RN
        FROM    FSCRM.TABLE_A A,
            (SELECT * FROM TABLE_C C WHERE C.SOMETHING='SOMETHING' AND C.NUM=1234) B 
        WHERE   A.COMPARE_CONDITION=B.COMPARE_CONDITION
        AND A.NUM NOT IN (1, 2, 3)
        AND NOT EXISTS (SELECT D.COLD FROM TABLE_D WHERE A.COLA=D.COLD)
        ORDER BY A.ORDER_CONDITION
    """

    with open('order_renew_cancel_daily.hql', 'r', encoding='utf8') as sql_file:
        #print(sql_file.read().strip())
        sqls = sqlparse.split(sql_file.read().strip())
        for sql in sqls:
            tables = ', '.join(extract_tables(sql))
            print('Tables: {}'.format(tables))

    #tables = ', '.join(extract_tables(sql1))
    #print('Tables: {}'.format(tables))
