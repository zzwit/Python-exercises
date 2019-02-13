import redis
import time
import pymysql.cursors
import  json
"""
redis 订阅者

"""

DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PWD = 'root'
DB_NAME = 'novel_collect'
DB_CHARSET = 'utf8'
DB_PORT = 3306
# # redis

m_mysql = pymysql.Connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PWD,
    port=DB_PORT,
    db=DB_NAME,
    charset=DB_CHARSET
)


rc = redis.StrictRedis(host='127.0.0.1', port='6379', db=3, password='')
m_mysql.cursorclass = pymysql.cursors.DictCursor
cursor = m_mysql.cursor()

# 获取的所有的相关的章节的最近更新的数量
novel_chapters_sql = "select * from  novel_novels_gather where id=2"
cursor.execute(novel_chapters_sql)
novel_chapters = cursor.fetchall()

#data = {"id": 39837, "new_novels_id": 334361, "name_author": "xiaowen", "name_author_que": "b8a7c2efb75336c8c4edf69ecdf2cb96", "novels_ids": "274071,319831,432315", "weight": 100, "form_novels_id": 274071, "source_ids": "", "chapters_numbers": "", "up_novels_id": 274071}

for val in novel_chapters:
    #val = str(val)
    val['up_novels_id'] = "165465"
    val = json.dumps(val,ensure_ascii=False)
    print(val)
    rc.publish("UPNOVELS:CHAPTER",val)
    exit()
#
# val = json.dumps(data, ensure_ascii=False)
# rc.publish("UPNOVELS:CHAPTER",val)

print(novel_chapters)
exit()
# for i in range(len(number_list)):
#     value_new = str(number_list[i]) + ' ' + str(signal[i])
#     rc.publish("liao", value_new)  #发
#     print("发送",value_new)
