#coding:utf-8

import pandas as pd
import  datetime
import calendar
import warnings
from sqlalchemy import create_engine
import sqlalchemy
from py2neo import Graph
import psycopg2
from neo4j.v1 import GraphDatabase
from string import Template
import pickle
import json


def fill_first_last():
    begin = datetime.datetime.now()
    # 补充首段和终段信息
    query_sql = """
          select * from $arg1 order by code,schedule_time 
          """
    query_sql = Template(query_sql)  # template方法
    df = pd.read_sql_query(query_sql.substitute(arg1='zhu_MultiSend_v2'), engine)  # 配合pandas的方法读取数据库值
    i = 0
    F_index = []
    L_index = []
    df = df.set_index(['code'])
    for uid in df.index.unique():
        i += 1
        print(i)
        tmp = df.loc[uid, 'shipment_segment']
        if 'F' in tmp.values:
            pass
        else:
            print(uid,tmp)
            df.loc[tmp.index[0], 'shipment_segment'][0:1] = 'F'
        if 'L' in tmp.values:
            pass
        else:
            print(uid,tmp)
            df.loc[tmp.index[-1], 'shipment_segment'][-1::] = 'L'

    df.reset_index(inplace=True)
    df.to_csv('pinche_neo4j.csv', index=False, encoding='utf-8')
    # df.to_csv('/var/lib/neo4j/import/pinche_neo4j.csv', encoding='utf-8')
    # dtype = {'aviabled_time': sqlalchemy.dialects.mysql.DATETIME(fsp=6),'schedule_time':sqlalchemy.dialects.mysql.DATETIME(fsp=6)}
    print('正在插入数据库')
    df.to_sql('zhu_MultiSend_v3', engine, if_exists='replace', index=False, chunksize=3000)
    print(df)
    end = datetime.datetime.now()
    print('补充首段和终段信息 ', end - begin)

def neo4j_import():
    # 导入数据到Neo4j数据库, 耗时1小时10分钟
    begin = datetime.datetime.now()
    graph.delete_all()
    session = driver.session()
    session.run('CREATE INDEX ON :City(name_id)')
    session.run(' CREATE INDEX ON :schedule(order_code)')
    importData = '''
    USING PERIODIC COMMIT 500
    LOAD CSV WITH HEADERS FROM "file:///pinche_neo4j.csv" AS csvLine
    MERGE (start:City { name:csvLine.shipment_src_wh_name,
    name_id:csvLine.shipment_src_wh_id })
    MERGE (end:City {
    name: csvLine.shipment_dest_wh_name,name_id:csvLine.shipment_dest_wh_id})
    MERGE (start)-[:schedule { order_code:csvLine.code,schedule_group_id: csvLine.schedule_group_id,trailer_id:csvLine.trailer_id,
    shipment_segment:csvLine.shipment_segment,schedule_time:csvLine.schedule_time,load_position_name:csvLine.load_position_name,load_position_id:csvLine.load_position_id }]->(end)
    '''
    session.run(importData)
    end = datetime.datetime.now()
    print('导入数据到neo4j ',end-begin)

# 去掉相邻的两个重复装卸地#################################
def unique_load(x):
    result = []
    for i in range(len(x)-1):
        if  x[i]!=x[i+1]:
            result.append(x[i])
        else:pass
    result.append(x[-1])

    return result

def pinche_city():
    '''

    MATCH p=(start:City)-[first:schedule{shipment_segment:'F'}]->(a)-[m:schedule*0..10]->(b)-[last:schedule{shipment_segment:'L'}]->(end:City)
        WHERE first.code = last.code and all(rel in m where rel.code=first.code )
        return first.code as order_id,start.name as order_start_name,end.name as order_dest_name,first.schedule_group_id as schedule_group_id, extract( r IN relationships(p)| r.load_position_name) as load_position_name,extract( r IN relationships(p)| r.load_position_id) as load_position_id

    :return:
    '''
    # 查找所有的拼车城市对by neo4j
    group_name = \
        '''
        MATCH p=(start:City)-[first:schedule{shipment_segment:'F'}]->(a)-[m:schedule*0..10]->(b)-[last:schedule{shipment_segment:'L'}]->(end:City)
        WHERE first.order_code = last.order_code and all(rel in m where rel.order_code=first.order_code )
        return first.order_code as order_id,start.name as order_start_name,end.name as order_dest_name,first.schedule_group_id as schedule_group_id, extract( r IN relationships(p)| r.load_position_name) as load_position_name,extract( r IN relationships(p)| r.load_position_id) as load_position_id 
        '''
    begin = datetime.datetime.now()
    pinche_info = []
    with driver.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(group_name)
            for record in result:

                    #print(record["m.schedule_group_id"])
                pinche_info.append([record['order_id'],record['order_start_name'],record['order_dest_name'],record['schedule_group_id'],record['load_position_name'],record['load_position_id']])
    end = datetime.datetime.now()
    print('运行时间 ', end-begin)
    # 耗时2h 33 min
    pinche_info = pd.DataFrame(pinche_info)
    pinche_info.columns = ['order_id','order_start_name','order_dest_name','schedule_group_id','load_position_name','load_position_id']
    id_parse = pinche_info.loc[:,'load_position_id'].apply(lambda x:unique_load(x))
    name_parse = pinche_info.loc[:,'load_position_name'].apply(lambda x:unique_load(x))
    pinche_info['load_id_parse'] = id_parse.apply(lambda x:'-'.join(x))
    pinche_info['load_name_parse'] = name_parse.apply(lambda x:'-'.join(x))
    pinche_info.to_csv('pinche_info.csv', encoding='utf-8', index = False)
    # 存储数据到pickle
    pickle_name = 'pinche_info.pkl'
    output = open(pickle_name, 'wb')
    pinche_dict = {'pinche_info': pinche_info}
    pickle.dump(pinche_dict, output)
    output.close()

    print('拼车城市团 ',len(list(set(pinche_info['load_id_parse']))))
    return pinche_info

def pinche_schedule_info():

    group_name = \
        '''
        MATCH p=(start:City)-[first:schedule{shipment_segment:'F'}]->(a)-[m:schedule*0..10]->(b)-[last:schedule{shipment_segment:'L'}]->(end:City)
        WHERE first.order_code = last.order_code and all(rel in m where rel.order_code=first.order_code )
        return first.order_code as order_id,start.name as order_start_name,end.name as order_dest_name,first.schedule_group_id as first_schedule_group_id, extract( r IN relationships(p)| r.schedule_group_id) as schedule_group_id,extract( r IN relationships(p)| r.name) as dest_name 
        '''
    begin = datetime.datetime.now()
    pinche_info = []
    with driver.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(group_name)
            for record in result:
                    #print(record["m.schedule_group_id"])
                pinche_info.append([record['order_id'],record['order_start_name'],record['order_dest_name'],record['first_schedule_group_id'],record['schedule_group_id'],record['dest_name']])
    end = datetime.datetime.now()
    print(end-begin)
    pinche_info = pd.DataFrame(pinche_info)
    pinche_info.columns = ['order_id', 'order_start_name', 'order_dest_name', 'first_schedule_group_id', 'schedule_group_id', 'dest_name']
    pinche_info.to_csv('pinche_info.csv',encoding='utf-8', index = False)
    return pinche_info

if __name__ == '__main__':
    # warnings.filterwarnings('ignore')
    # neo4j连接字符串1
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "123456"))
    # neo4j链接字符串2
    graph = Graph("http://localhost:7474", username="neo4j", password="123456")
    # mysql连接字符串类型1
    parameter = {'user': 'root', 'passwd': '123456', 'host': '127.0.0.1', 'port': 3306,
                'db': 'pinche', 'charset': 'utf8'}
    engine = create_engine('mysql+pymysql://' + parameter['user'] + ':' + parameter['passwd'] + '@' +
                parameter['host'] + ':' + str(parameter['port']) + '/' + parameter['db']+'?charset=utf8', echo_pool=True, pool_recycle=3600)
    # mysql链接字符串类型2
    #conn = MySQLdb.connect(**parameter)
    #cursor = conn.cursor()
    print('清洗工作开始')
    # 补充首短终段信息
    # fill_first_last()
    # 导入数据到neo4j图数据库,760M大小的数据库
    # neo4j_import()
    # 查找所有的拼车城市对
    #pinche_info_neo4j = pinche_city()
    # 从pickle读取拼车数据
    print('读取数据')
    pickle_name = 'pinche_info.pkl'
    pkl_file = open(pickle_name, 'rb')
    pinche_dict = pickle.load(pkl_file)
    pinche_info_neo4j = pinche_dict['pinche_info']
    # 最常使用的拼单组合###########################
    print('取最长订单')
    pinche_info_neo4j['pinche_length'] = pinche_info_neo4j['load_id_parse'].apply(lambda x: x.count('-') + 1)
    pinche_group = pinche_info_neo4j.loc[
        pinche_info_neo4j.reset_index().groupby(['schedule_group_id'])['pinche_length'].idxmax()]

    ############################################
    # 在Neo4j中删除冗余的短订单记录################
    print('删除短订单')
    a = pinche_group['order_id'].values
    b = pinche_info_neo4j['order_id'].values
    residual_order = list(set(a) ^ set(b))
    delete_resi = '''
        match (a)-[r]->(b) where r.order_code in $resi delete r
        '''
    with driver.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(delete_resi, resi=residual_order)
    # 1 删除关系，2 参数用法 3 Python 参数用法
    # 根据装载数量关系剔除前段拼车数据
    # TODO:Tuesday's task
    # 装车数量分布##################################

    pinche_schedule = pinche_schedule_info()
    # pinche_schedule = pd.read_csv('pinche_schedule_info_0629.csv')
    print(pinche_schedule)

    pinche_schedule['pinche_length'] = pinche_schedule['schedule_group_id'].apply(lambda x: len(x))
    old_len = len(pinche_schedule)
    pinche_schedule = pinche_schedule.loc[
        pinche_schedule.reset_index().groupby(['first_schedule_group_id'])['pinche_length'].idxmax()]
    print('总共涉及(%s)次拼车', (old_len))
    if len(pinche_schedule) == old_len:
        print('短订单清楚完毕')
    else:
        print('短订单尚有留存，共剩余(%s)个', (old_len - len(pinche_schedule)))

    load_number = []
    query_sql = """
        select schedule_group_id, count(schedule_group_id) as num_sche from $arg1 group by schedule_group_id          """
    query_sql = Template(query_sql)  # template方法
    df = pd.read_sql_query(query_sql.substitute(arg1='pinche_all_20171117'), engine)  # 配合pandas的方法读取数据库值
    i = 0
    for schedule_id in pinche_schedule['schedule_group_id']:
        print(i)
        i += 1
        temp = []
        for sche_id in schedule_id:
            if len(df[df.schedule_group_id == int(sche_id)]['num_sche']) == 0:
                temp.append(0)
            else:
                temp.append(df[df.schedule_group_id == int(sche_id)]['num_sche'].values[0])
        load_number.append(temp)
    pinche_schedule['load_number'] = load_number
    pinche_schedule.to_csv('load_number.csv',encoding='utf-8', index = False)


    # 查找前段拼车,前段拼车定义：大板车装载数量存在上升行为,然后去除前段拼车数据；同时去除装载数量不变的拼车行为，此部分数据可能有异常
    # pinche_schedule = pd.read_csv('./result_data/load_number.csv',encoding='utf-8')
    # pinche_schedule['load_number'] = pinche_schedule['load_number'].apply(json.loads)
    temp = []
    for item in pinche_schedule['load_number']:
        qd_pin = 0
        for i in range(len(item) - 1):
            if item[i + 1] >= item[i]:
                qd_pin = 1
                # 识别到前段拼车
                break
        temp.append(qd_pin)
    pinche_schedule['qd_pin'] = temp
    qd_order = pinche_schedule[pinche_schedule.qd_pin == 0]
    print('一共发现(%s)次hou段拼车' % (len(qd_order)))
    # 在neo4j中删除hou段拼车记录
    residual_order = list(qd_order['order_id'].values)
    delete_resi = '''
        match (a)-[r]->(b) where r.order_code in $resi delete r
        '''
    with driver.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(delete_resi, resi=residual_order)
    # 在pinche_group中删除hou段拼车
    pinche_group = pinche_group[pinche_group['order_id'].isin(residual_order) == False]
    pinche_group['load_position_id'] = pinche_group['load_position_id'].apply(json.dumps)
    pinche_group['load_position_name'] = pinche_group['load_position_name'].apply(json.dumps)
    pinche_group['schedule_group_id'] = pinche_group['schedule_group_id'].apply(json.dumps)

    pinche_group.to_sql('zhu_Multi_Send_overall', engine, if_exists='replace', index=False, chunksize=3000)


