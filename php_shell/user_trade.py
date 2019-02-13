import pymysql.cursors
import math
import redis

import json
import difflib

from multiprocessing import Pool
import logging
import os, time, random
from datetime import datetime

# DB_HOST = '221.195.1.228'
# DB_USER = 'zhaozhiwen'
# DB_PWD = 'ERhjkp%!698dDIW'
# DB_NAME = 'ksdatas'
# DB_CHARSET = 'utf8'
# DB_PORT = 3306


DB_HOST = '192.168.2.1'
DB_USER = 'codeuser'
DB_PWD = 'RvZ@7^yGR2waQNLJ'
DB_NAME = 'ksdatas'
DB_CHARSET = 'utf8'
DB_PORT = 3306

m_mysql = pymysql.Connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PWD,
    port=DB_PORT,
    db=DB_NAME,
    charset=DB_CHARSET
)


def divide_sell():
    count = 2746248
    page_tount = math.ceil(count / 10000)
    cursor = m_mysql.cursor()
    for i in range(page_tount):
        inset_sql = "INSERT INTO ks_user_trade_log_2018 select * from ks_user_trade_log where year=2018 limit 10000"
        cursor.execute(inset_sql)
        m_mysql.commit()
        delect_sql = "DELETE from ks_user_trade_log where year=2018 limit 10000"
        print(delect_sql)
        cursor.execute(delect_sql)
        m_mysql.commit()
        time.sleep(0.3)


    cursor.close()
    print("处理完毕")


def get_userList():
     cursor = m_mysql.cursor()
     count_sql = "select count(1) from ks_user"
     cursor.execute(count_sql)
     count = cursor.fetchone()
     count = count[0]
     page_tount = math.ceil(count / 1000)
     for i in range(page_tount):
         limit_count = i*1000
         user_sql = "select id from ks_user limit %d,1000" %(limit_count)
         cursor.execute(user_sql)
         userList = cursor.fetchall()
         for index,val in enumerate(userList):
             user_id = val[0]
             sqls = "select from ks_user_trade_log where user_id=%s group by date_time" %(user_id)
             print(user_id)
             exit()


    #inset_sql = "INSERT INTO ks_user_trade_log_2018 select * from ks_user_trade_log where year=2018" %

if __name__=='__main__':

    divide_sell()