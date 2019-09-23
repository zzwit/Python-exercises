"""
    同步用户阅读的时间
"""

import pymysql.cursors
import redis
import datetime
import json
import phpserialize
import time
from multiprocessing import Pool


import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"




today = datetime.date.today()
todayYas = today.strftime('%Y-%m-%d')  # 格式化时间 获取的昨天的时间
date_time = today.strftime('%Y%m%d')  # 格式化时间 获取的昨天的时间
logName = "/data/www/iqt_script/logs/novel_php_book_time%s.log" %(todayYas)
logging.basicConfig(filename=logName, level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)


# Mysql
DB_HOST = '127.0.0.1'
DB_USER = 'xiaow'
DB_PWD = 'xiaowe'
DB_NAME = 'appps'
DB_CHARSET = 'utf8'
DB_PORT = 3306


# Redis
redis_config = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 2,
    "password":"ZrjhRedis@)!("

}

redis_pool = redis.ConnectionPool(**redis_config)
myRedis = redis.Redis(connection_pool=redis_pool)

my_sql = pymysql.Connect(
            host = DB_HOST,
            user = DB_USER,
            passwd= DB_PWD,
            port = DB_PORT,
            db = DB_NAME,
            charset=DB_CHARSET
        )

class userSyncTime(object):
    def __init__(self):
        self.mysql = my_sql.cursor(cursor=pymysql.cursors.DictCursor)
        self.sredis = redis.Redis(connection_pool=redis_pool)

    # 获取的Redis当亲的用户的阅读时间
    def sync_data(self,user_id,read_time):
        seltsql = "select id from iqy_readbook_time_log where user_id=%s and date_time=%s" %(user_id,date_time)
        self.mysql.execute(seltsql)
        info = self.mysql.fetchone()
        if info :
            read_id = info.get("id")
        else:
            read_id = 0
        if read_id > 0 :
            sql = "update iqy_readbook_time_log set read_time=%s,created_at=%s where id=%s " %(read_time,int(time.time()),read_id)
        else :
            sql = "insert into iqy_readbook_time_log (user_id,date_time,created_at,read_time) values(%s,%s,%s,%s)" %(user_id,date_time,int(time.time()),read_time)
        self.mysql.execute(sql)
        my_sql.commit()
        self.mysql.close()

def run_task(user_id,read_time):
    nnovel_model = userSyncTime()
    nnovel_model.sync_data(user_id,read_time)


if __name__ == '__main__':
    p = Pool(2)
    userBookTimeKey = "USER:READ:BOOK:TOADY:TIME:"+todayYas
    userBookTimeList = myRedis.hgetall(userBookTimeKey)
    for keyx in userBookTimeList:
        user_id = int(keyx)  #用户ID
        times = phpserialize.loads(userBookTimeList[keyx]) #时间秒数

        if times > 0 :
            p.apply_async(run_task, args=(user_id, times,))

    p.close()
    p.join()
    inlog = "在%s同步" %(int(time.time()))
    logging.info(inlog)