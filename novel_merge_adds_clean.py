"""
 合并小说的内容
"""
import pymysql.cursors
import redis
import hashlib
import os
import json
import time
import difflib
import sys
# from pymongo import MongoClient
from multiprocessing import Pool

import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='/data/www/novel_script/logs/novel_mage_list.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)

DB_HOST = '192.168.0.18'
DB_USER = 'novelcode'
DB_PWD = 'RvZ@7^yGR2waQNLJ'
DB_NAME = 'novel_collect'
DB_CHARSET = 'utf8'
DB_PORT = 3306


redis_config = {
    "host": "61.160.196.39",
    "port": 26890,
    "db": 4,
    "password": 'DH56ji2(3u^4',
}
#
#
#
# MG_HOST = "221.195.1.228"
# MG_PORT = 27017
# MG_USER = "admin"
# MG_PWD  = "admin@1234"
# host = '221.195.1.228'
# client = MongoClient(host, 27017,username="admin",password="admin@1234")



# DB_HOST = '127.0.0.1'
# DB_USER = 'root'
# DB_PWD = 'root'
# DB_NAME = 'novel_collect'
# DB_CHARSET = 'utf8'
# DB_PORT = 3306
# # redis
#
# redis_config = {
#     "host": "127.0.0.1",
#     "port": 6379,
#     "db": 4,
#     "password": '',
# }

# MG_HOST = "221.195.1.228"
# MG_PORT = 27017
# MG_USER = "admin"
# MG_PWD  = "admin@1234"
# host = '221.195.1.228'
# client = MongoClient(host, 27017,username="admin",password="admin@1234")

#文件的根目录的地址
FILE_BASE = "/data/book/"
#站点的ID
SOURCE_ID = 555333


my_sql = pymysql.Connect(
            host = DB_HOST,
            user = DB_USER,
            passwd= DB_PWD,
            port = DB_PORT,
            db = DB_NAME,
            charset=DB_CHARSET
        )

class novelMarge(object):

    def __init__(self):
        self.mysql = my_sql

        # self.redis_pool = redis.ConnectionPool(**redis_config)
        # self.sredis = redis.Redis(connection_pool=self.redis_pool)

        # mg_client = MongoClient(MG_HOST, MG_PORT, username=MG_USER, password=MG_PWD)

    """
        合并新书必须拷贝章节内容
    """
    # def add_new_novels(self):
    #     # if self.sredis.get("add_new_novels"):
    #     #     print("程序在进行中。请等待。。。")
    #     #     exit()
    #     # else:
    #     #     self.sredis.set("add_new_novels", 1)
    #
    #     lg = 0
    #     page_index = 0
    #     limit = 1000
    #     cursor = self.mysql.cursor()
    #
    #     # try:
    #     while lg == 0:
    #         page = page_index * limit
    #         sql = "select id,novels_ids,new_novels_id from `novel_novels_gather` where new_novels_id>0 limit %d,%d" % (page, limit)
    #         print(sql)
    #         cursor.execute(sql)
    #         gather_list = cursor.fetchall()
    #
    #         lencut = len(gather_list)
    #         if lencut == 0:
    #             lg = 1
    #             cursor.close()
    #             self.sredis.delete("add_new_novels")
    #             exit()
    #         else:
    #             if gather_list:
    #                 page_index +=1
    #
    #                 for val in gather_list:
    #                     #合并新的ID
    #                     new_novel_id = val[2]
    #
    #                     #获取小说的第一个站点
    #                     novel_id_sql = "select id,source_id from novel_novels_neaten where source_id in(122,123,124,62,93,3,1,82,15,4) and id in(%s) limit 1" %(val[1])
    #                     cursor.execute(novel_id_sql)
    #                     novels_ids = cursor.fetchone()
    #                     form_source_id = 0
    #                     print(novel_id_sql)
    #                     if novels_ids == None:
    #                         recs = val[1].split(',')
    #                         novels_ids = recs[0]
    #                     else:
    #                         form_source_id =  novels_ids[1]
    #                         novels_ids = novels_ids[0]
    #                     if form_source_id == 0:
    #                         novel_novels_sql = "select source_id from novel_novels_neaten where novels_id=%d " %(novels_ids)
    #                         cursor.execute(novel_novels_sql)
    #                         novels_info = cursor.fetchone()
    #                         if novels_info :
    #                             form_source_id = novels_info[0]
    #
    #
    #                     if form_source_id:
    #                         fromPath = "%s%s/%s/chapter.json" % (FILE_BASE,form_source_id,novels_ids)
    #                         # 判断文件是否存在
    #                         is_file_path = os.path.isfile(fromPath)
    #                         if is_file_path == False:
    #                             print("没有目录")
    #                             continue
    #                         else:
    #                             print(fromPath)
    #                             with open(fromPath, 'r') as f:
    #                                     flist = f.read()
    #                                     if flist:
    #                                         print("新版分析")
    #                                         chapter_list = json.loads(flist)
    #
    #                                     else:
    #                                         chapter_list = ""
    #                                         print("目录没有内容")
    #                             if new_novel_id:
    #                                 #数据写入的到Mongo表里
    #                                 newsPath = "%s%s/%s/" % (FILE_BASE, SOURCE_ID,new_novel_id)
    #                                 newsFilePath = newsPath+"chapter.json"
    #
    #                                 #novel_chapters_path = "%s/%s/%s" %(SOURCE_ID,new_novel_id,"chapter.json")
    #                                 if is_file_path == True:
    #                                     new_rec = os.path.exists(newsPath)
    #                                     if new_rec == False:
    #                                         os.makedirs(newsPath)
    #                                     #json.dumps()
    #                                     new_chapter = []
    #                                     if chapter_list:
    #                                         for chapter in chapter_list:
    #                                             new_chapters = {}
    #                                             chapters_list_data = {}
    #                                             #目录信息
    #                                             new_chapters['_id'] = chapter['_id']
    #                                             new_chapters['name'] = chapter['name']
    #                                             new_chapters['add_time'] = chapter['add_time']
    #                                             #章节的信息
    #                                             chapters_list_data['source_id'] = form_source_id
    #                                             chapters_list_data['url'] = chapter['url']
    #                                             chapters_list_data['add_time'] = chapter['add_time']
    #                                             chapters_list_data['weight'] = 1
    #
    #                                             self.add_novel_chapter_list(new_novel_id, chapter['_id'],chapters_list_data)
    #                                             new_chapter.append(new_chapters)
    #                                             #最新章节
    #                                             new_chapter_name = new_chapters['name']
    #                                         upcount = len(new_chapter)
    #
    #                                         with open(newsFilePath, 'w') as f:
    #                                              f.write(json.dumps(new_chapter,ensure_ascii=False))
    #
    #                                         # neaten_update_sql = "update novel_novels_neaten set count=%d where novels_id=%d" %(upcount,new_novel_id)
    #                                         # cursor.execute(neaten_update_sql)
    #                                         # self.mysql.commit()
    #
    #                                         #添加的更新的最新章节的信息
    #                                         # update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #                                         # chapters_up_sql = 'update novel_chapters set is_sync=0,count=%s,new_name="%s",update_time="%s" where novels_id=%s' % ( upcount, new_chapter_name, update_time, new_novel_id)
    #                                         #
    #                                         # cursor.execute(chapters_up_sql)
    #                                         msg = "生成新的目录小说ID：%s" % (new_novel_id)
    #                                         print(msg)
    #                                         #self.mysql.commit()
    #                                     else:
    #                                         print("暂无数据")
    #                                     print("生成成功")
    #                             else:
    #                                 print("新增数据失败")
    #
    #                     else:
    #                         print("不存在")
    #             else:
    #                 print("没有相似小说")
    #     # except Exception as e:
    #     #     print(e)
    #     #     print("财务信息")
    #     #     self.sredis.delete("add_new_novels")
    #     #     exit()
    #
    #     self.sredis.delete("add_new_novels")
    #     exit()

    """
           合并新书必须拷贝章节内容
    """

    def add_new_novels(self,gather_list):
        # if self.sredis.get("add_new_novels"):
        #     print("程序在进行中。请等待。。。")
        #     exit()
        # else:
        #     self.sredis.set("add_new_novels", 1)

        # try:
        cursor = self.mysql.cursor()
        lencut = len(gather_list)
        if lencut == 0:
            lg = 1
            cursor.close()
            self.sredis.delete("add_new_novels")
        else:
            if gather_list:
                val = gather_list

                # 合并新的ID
                new_novel_id = val[2]

                # 获取小说的第一个站点
                novel_id_sql = "select novels_id,source_id from novel_novels_neaten where source_id in(122,123,124,62,93,3,1,82,15,4) and novels_id in(%s) limit 1" % (val[1])
                cursor.execute(novel_id_sql)
                novels_ids = cursor.fetchone()
                form_source_id = 0

                if novels_ids == None:
                    recs = val[1].split(',')
                    novels_ids = recs[0]
                else:
                    form_source_id = novels_ids[1]
                    novels_ids = novels_ids[0]


                if form_source_id == 0:
                    novel_novels_sql = "select source_id from novel_novels_neaten where novels_id=%s " % (novels_ids)
                    cursor.execute(novel_novels_sql)
                    novels_info = cursor.fetchone()
                    if novels_info:
                        form_source_id = novels_info[0]

                if form_source_id:
                    fromPath = "%s%s/%s/chapter.json" % (FILE_BASE, form_source_id, novels_ids)
                    msgss = "分析来源的小说ID：%s" %(novels_ids)
                    logging.info(msgss)
                    # 判断文件是否存在
                    is_file_path = os.path.isfile(fromPath)
                    if is_file_path == False:
                        logging.info("没有目录")
                    else:
                        with open(fromPath, 'r') as f:
                            flist = f.read()
                            if flist:

                                chapter_list = json.loads(flist)

                            else:
                                chapter_list = ""
                                logging.info("目录没有内容")
                        if new_novel_id:
                            # 数据写入的到Mongo表里
                            newsPath = "%s%s/%s/" % (FILE_BASE, SOURCE_ID, new_novel_id)
                            newsFilePath = newsPath + "chapter.json"

                            # novel_chapters_path = "%s/%s/%s" %(SOURCE_ID,new_novel_id,"chapter.json")
                            if is_file_path == True:
                                new_rec = os.path.exists(newsPath)
                                if new_rec == False:
                                    os.makedirs(newsPath)
                                # json.dumps()
                                new_chapter = []
                                if chapter_list:
                                    for chapter in chapter_list:
                                        new_chapters = {}
                                        chapters_list_data = {}
                                        # 目录信息
                                        new_chapters['_id'] = chapter['_id']
                                        new_chapters['name'] = chapter['name']
                                        new_chapters['add_time'] = chapter['add_time']
                                        # 章节的信息
                                        chapters_list_data['source_id'] = form_source_id
                                        chapters_list_data['url'] = chapter['url']
                                        chapters_list_data['add_time'] = chapter['add_time']
                                        chapters_list_data['weight'] = 1

                                        self.add_novel_chapter_list(new_novel_id, chapter['_id'],
                                                                    chapters_list_data)
                                        new_chapter.append(new_chapters)
                                        # 最新章节
                                        new_chapter_name = new_chapters['name']
                                    upcount = len(new_chapter)

                                    with open(newsFilePath, 'w') as f:
                                        f.write(json.dumps(new_chapter, ensure_ascii=False))

                                    # neaten_update_sql = "update novel_novels_neaten set count=%d where novels_id=%d" %(upcount,new_novel_id)
                                    # cursor.execute(neaten_update_sql)
                                    # self.mysql.commit()

                                    # 添加的更新的最新章节的信息
                                    # update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                                    # chapters_up_sql = 'update novel_chapters set is_sync=0,count=%s,new_name="%s",update_time="%s" where novels_id=%s' % ( upcount, new_chapter_name, update_time, new_novel_id)
                                    #
                                    # cursor.execute(chapters_up_sql)
                                    msg = "生成新的目录小说ID：%s" % (new_novel_id)
                                    logging.info(msg)
                                    # self.mysql.commit()
                                else:
                                    logging.info("暂无数据")
                                logging.info("生成完成")
                        else:
                            print("新增数据失败")

                else:
                    print("不存在")
            else:
                logging.info("没有相似小说")
        # except Exception as e:
        #     msg = "没有相似小说.小说报错:%s" %(new_novel_id)
        #     logging.info(msg)


    """
     生成的子目录信息
     source_id 站点ID
     novels_id 书籍ID
     chapter_id 章节ID
     data 列表的字典
    """
    def add_novel_chapter_list(self,novels_id,chapter_id,data):

        path = "%s%s/%s/" % (FILE_BASE, SOURCE_ID, novels_id)
        file_path = path + "list_"+str(chapter_id)+".json"
        rec = os.path.exists(file_path)
        list_data = []
        if rec == True:
            with open(file_path,'r') as fr:
                lists = fr.read()
                if lists:
                    list_data = json.loads(lists)
                else:
                    list_data = []
        with open(file_path,mode='w') as f:
            if rec == True:
                source_id_rel = 0
                for ival in list_data:
                    if data['source_id'] == ival['source_id']:
                        source_id_rel = 1
                if source_id_rel == 0:
                    list_data.append(data)
            else:
                list_data.append(data)
            f.write(json.dumps(list_data,ensure_ascii=False))
        return 1



def run_task(sindex,novels_list):
    novel_mage_model = novelMarge()
    novel_mage_model.add_new_novels(novels_list)


if __name__ == '__main__':
    #sys_name = sys.argv[1]
    #print('Parent process %s.' % os.getpid())
    # 开启 进程
    p = Pool(6)
    # 多少任务
    my_cursor = my_sql.cursor()
    #sql = "select id,novels_ids,new_novels_id from `novel_novels_gather` where new_novels_id>0 limit 90000,30000"

    sql ="select c.`id`,c.`novels_ids`,c.`new_novels_id` from novel_chapters as a left join novel_novels as b on a.novels_id =b.id left join novel_novels_gather as c on c.`new_novels_id` = b.id  where b.source_id =35 and a.update_time like '2019-01-26%'"
    my_cursor.execute(sql)
    gather_list = my_cursor.fetchall()
    my_cursor.close()

    logging.info("程序开始......")
    for index,novelss in enumerate(gather_list):
        ss = "在处理新书籍ID %s 更新数量 %s" %(novelss[1],index)
        logging.info(ss)
        with open("/data/www/novel_script/logs/coun.txt", "a+") as file_error:
            error_newsPath = str(index) + '\n'
            file_error.write(error_newsPath)

        p.apply_async(run_task, args=(index,novelss,))

    p.close()
    p.join()
    inlog = "数据处理完成，总共处理%s 条数据" % (len(gather_list))
    print(inlog)
    logging.info(inlog)