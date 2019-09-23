
"""
 合并小说的内容
"""
import pymysql.cursors
import redis
import hashlib
import shutil
import json
import difflib
import sys
from multiprocessing import Pool
import logging
import os, time, random

DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PWD = 'root'
DB_NAME = 'novel_collect'
DB_CHARSET = 'utf8'
DB_PORT = 3306
# redis

redis_config = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 4,
    "password": '',
}



m_mysql = pymysql.Connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PWD,
    port=DB_PORT,
    db=DB_NAME,
    charset=DB_CHARSET
)


class novelMarge(object):

    def __init__(self):
        self.mysql =  m_mysql


    """
        获取所有的来源 地址数据 进行书籍匹配和合并 匹配到 90 以上
    """

    def updas (self):
        lg = 0
        page_index = 0
        limit = 1000
        cursor = self.mysql.cursor()
        while lg == 0:
            page = page_index * limit
            sql = "select new_novels_id from novel_novels_gather where new_novels_id > 0 order by id asc limit %d,1000" % (page)
            cursor.execute(sql)
            novels_ids = cursor.fetchall()
            lencut = len(novels_ids)
            page_index += 1
            if lencut == 0:
                lg = 1
                print("全部更新成功")
                cursor.close()
            else:
                novel_ids = ""

                for index,idss in enumerate(novels_ids):
                    if index == 0 :
                        novel_ids = str(idss[0])
                    else:
                        novel_ids += ",%s" %(idss[0])

                if novel_ids:
                    novel_chapters_sql = "update novel_chapters set is_sync = 0 where novels_id IN(%s)" % (novel_ids)
                    cursor.execute(novel_chapters_sql)
                    self.mysql.commit()
                else:
                    print("没有更新的数据。。。。")






if __name__=='__main__':
    amodel = novelMarge()
    amodel.updas()



