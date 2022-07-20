import configparser
import pymysql

# 读取配置文件
conf = configparser.RawConfigParser()
conf.read("conf.ini")
# 获取源数据库参数
sourceDBUrl  = str(conf.get('soureDB', 'sourcedburl'))
sourceDBUser = str(conf.get('soureDB', 'sourcedbuser'))
sourceDBKey =  str(conf.get('soureDB', 'sourcedbkey'))
sourceDataBse = str(conf.get('soureDB', 'sourcedatabase'))
# 获取目标数据库参数
targetdburl =str(conf.get('targetDB', 'targetdburl'))
targetdbuser =str(conf.get('targetDB', 'targetdbuser'))
targetdbkey =str(conf.get('targetDB', 'targetdbkey'))
targetdbs =str(conf.get('targetDB', 'targetdbs'))

#链接源数据库
conn = pymysql.connect(host=sourceDBUrl, user=sourceDBUser, passwd=sourceDBKey, db=sourceDataBse, charset='utf8')
cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
#链接目标数据库
conn2 = pymysql.connect(host=targetdburl, user=targetdbuser, passwd=targetdbkey, db=targetdbs, charset='utf8')
cur2 = conn2.cursor()
#读取sql文件
file=open("sql.txt",mode='r',encoding='utf-8')
sql="".join(file.readlines())
# 返回受影响的行数
cur.execute(sql)
# 返回数据,返回的是tuple类型
res = cur.fetchall()
# 获取目标表名和字段
table = str(conf.get('targetDB', 'insert_table'))
colums = str(conf.get('targetDB', 'insert_colums'))
# 源表字段
source_colums = str(conf.get('soureDB', 'source_colums'))
# 插入sql
insert_sql = "insert into "+ table +'('+ colums +')'+ " values("+("%s,"*(len(colums.split(","))))[:-1]+");"
# insert_sql = "insert into "+ table +'('+ colums +')'+ " values("+type_colums+");"
a_list=[]
#拼写插入参数
for item in res:
    args = []
    for i in source_colums.split(","):
        args.append(str(item[i]))
    a_list.append(tuple(args))
    # 分片插入数据
    if len(a_list) == 1000:
        cur2 = conn2.cursor()
        cur2.executemany(insert_sql,a_list)
        conn2.commit()
# 剩余数据插入
cur2 = conn2.cursor()
cur2.executemany(insert_sql,a_list)
conn2.commit()


cur.close()
conn.close()
cur2.close()
conn2.close()
