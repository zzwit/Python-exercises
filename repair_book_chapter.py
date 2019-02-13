import pymysql.cursors
import redis
import hashlib
import shutil
import os
import json
import time
import difflib
import sys

"""
author:xiaowen
主要是针对合并站书籍进行修复功能
"""


# 线上
#Mysql
DB_HOST = '58.216.10.18'
DB_USER = 'novelcode'
DB_PWD = 'RvZ@7^yGR2waQNLJ'
DB_NAME = 'novel_collect'
DB_CHARSET = 'utf8'
DB_PORT = 3306
# Redis
REDIS_HOST = "61.160.196.39"
REDIS_PORT = 26890
REDIS_DB = 3
REDIS_PWD = 'DH56ji2(3u^4'


# DB_HOST = '127.0.0.1'
# DB_USER = 'root'
# DB_PWD = 'root'
# DB_NAME = 'novel_collect'
# DB_CHARSET = 'utf8'
# DB_PORT = 3306
# # redis
#
redis_config = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 4,
    "password": '',
}


FILE_BASE = "/data/book/"

SOURCE_ID = 35
class novelMarge(object):

    def __init__(self):
        self.mysql = pymysql.Connect(
            host = DB_HOST,
            user = DB_USER,
            passwd= DB_PWD,
            port = DB_PORT,
            db = DB_NAME,
            charset=DB_CHARSET
        )

        self.redis_pool = redis.ConnectionPool(**redis_config)
        self.sredis = redis.Redis(connection_pool=self.redis_pool)

    """
        只针对 35 站点
        novel_id 原来的小说ID
        new_novel_id 新小说ID
    """
    def repair_chapter(self,novel_id,new_novel_id):
        cursor = self.mysql.cursor()

        novel_new_sql = "select id from  novel_novels where id = %s" % (novel_id)
        cursor.execute(novel_new_sql)
        novel_new_info = cursor.fetchone()
        if novel_new_info == None:
            print("没有当前的书籍:",novel_id)
            exit()
            #new_novel_id
        # 获取的他的一些来源

        novel_novels_neaten_sql = "select novels_id,source_id,count from  novel_novels_neaten where novels_id = %s" % (new_novel_id)
        cursor.execute(novel_novels_neaten_sql)
        from_novel_info = cursor.fetchone()
        if from_novel_info == None:
            print("第二书籍ID",new_novel_id,"不存在")
            exit()

        from_source_id = from_novel_info[1]
        # 获取的原的信息
        form_path_list = "%s%s/%s/chapter.json" % (FILE_BASE, from_source_id,new_novel_id)
        print(form_path_list)

        form_recs = os.path.exists(form_path_list)
        if form_recs == True:


            # 获取的合并的书籍所有的章节内容
            newsPath = "%s%s/%s/chapter.json" % (FILE_BASE,SOURCE_ID, novel_id)

            #获取的内容
            with open(form_path_list, 'r') as f:
                from_chapters_list = f.read()
                from_chapters_list = json.loads(from_chapters_list)

            if from_chapters_list :
                chapter_id = 0
                mage_chapters_list = []
                for index, vs in enumerate(from_chapters_list):
                    chapter_id += 1
                    chapters_list_data = {}
                    chapters_list_ads = {}
                    # 更新的新的章节
                    chapters_list_data['source_id'] = from_source_id
                    chapters_list_data['url'] = vs['url']
                    chapters_list_data['add_time'] = vs['add_time']
                    chapters_list_data['weight'] = 1

                    chapters_list_ads['_id'] = chapter_id
                    chapters_list_ads['name'] = vs['name']
                    chapters_list_ads['add_time'] = vs['add_time']

                    self.add_novel_chapter_list(novel_id, chapter_id, chapters_list_data)
                    mage_chapters_list.append(chapters_list_ads)
                    chapters_new_name = vs['name']


                if mage_chapters_list:
                    with open(newsPath, 'w') as f:
                        f.write(json.dumps(mage_chapters_list, ensure_ascii=False))

                    cursor = self.mysql.cursor()
                    chapters_update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    count = chapter_id
                    chapters_up_sql = 'update novel_chapters set is_sync=0,count=%s,new_name="%s",update_time="%s" where novels_id=%s' % (
                        count, chapters_new_name, chapters_update_time, novel_id)

                    cursor.execute(chapters_up_sql)
                    self.mysql.commit()
                    #self.source_novel_chapter(novel_id,mage_chapters_list,novel_id)
                    print("更新成功")


        else:
            print("没有的数据")

    """
        更新的站点源的信息
    """
    def source_novel_chapter(self,novel_id,mage_chapters_list,mage_novel_id):

        print(mage_chapters_list)
        exit()
        cursor = self.mysql.cursor()
        # 获取当前的更新的
        novel_gather_sql = "select novels_ids from novel_novels_gather where new_novels_id=%s" % (novel_id)
        cursor.execute(novel_gather_sql)
        mage_novel_info = cursor.fetchone()
        # 查看一些来源的一些基本信息
        novel_new_sql = "select id,source_id from  novel_novels where has_content=1 and id IN(%s)" % (
            mage_novel_info[0])
        cursor.execute(novel_new_sql)
        mage_novel_source_list = cursor.fetchall()
        if mage_novel_source_list:
            for index,vals in enumerate(mage_novel_source_list):
                self.source_chapterlist(vals[0],vals[1],mage_chapters_list,mage_novel_id)


    # def source_chapterlist(self,novel_id,source_id,mage_chapters_list,mage_novel_id):
    #
    #     file_path = "%s%s/%s/chapter.json" % (FILE_BASE, source_id, novel_id)
    #     rec = os.path.exists(file_path)
    #     if rec == False:
    #         return 1
    #     else:
    #         list_data = []
    #
    #         with open(file_path, "r") as mage_chapters_f:
    #             from_chapters_file_list = mage_chapters_f.read()
    #             form_chapters_limit = json.loads(from_chapters_file_list)
    #
    #         for index, mage_vals in enumerate(mage_chapters_list):
    #
    #             minnumber = int(index - 10) if int(index - 10) > 0 else 0
    #             maxnumber = int(index + 6)
    #
    #             # 原网站的章节列表
    #             for index, form_valus in enumerate(form_chapters_limit[minnumber:maxnumber]):
    #                 rate = 0
    #                 if mage_vals.get('name') and form_valus.get('name'):
    #                     mage_vals_name = self.changeChineseNumToArab(mage_vals['name'])
    #                     form_valus_name = self.changeChineseNumToArab(form_valus['name'])
    #                     rate = difflib.SequenceMatcher(None, mage_vals_name, form_valus_name).quick_ratio()
    #                     rate = int(round(rate, 2) * 100)
    #                 if rate > 90:
    #                     form_chapters_file_list = {}
    #
    #                     # 合并之后的数据
    #                     mage_chapters_list_file_path = "%s%s/%s/%d/list.json" % (FILE_BASE, SOURCE_ID, mage_novel_id, mage_vals['_id'])
    #
    #                     file_status = os.access(mage_chapters_list_file_path, os.R_OK | os.W_OK | os.X_OK)
    #                     if file_status == False:
    #                         # file_error_path = "/data/www/novel_script/logs/book_power_%s.log" %(time.strftime("%Y-%m-%d", time.localtime()))
    #                         # with open(file_error_path, "a+") as file_error:
    #                         #     error_book_id = ','+str(mage_novel_id)
    #                         #     file_error.write(error_book_id)
    #                         continue
    #                     mage_chapters_path_recss = os.path.exists(mage_chapters_list_file_path)
    #
    #                     if mage_chapters_path_recss == False:
    #                         continue
    #
    #                     with open(mage_chapters_list_file_path, "r") as mage_chapters_f:
    #                         mage_chapters_file_list = mage_chapters_f.read()
    #                         mage_chapters_file_list = json.loads(mage_chapters_file_list)
    #
    #                     # 检查来源的是否存在的了
    #                     is_status = 0
    #                     for vs in mage_chapters_file_list:
    #                         if vs['source_id'] == form_source_id:
    #                             is_status = 1
    #
    #                     if is_status == 0:
    #                         # 来源的站点的数据整合
    #                         form_chapters_file_list['source_id'] = form_source_id
    #                         form_chapters_file_list['url'] = form_valus['url']
    #                         form_chapters_file_list['add_time'] = form_valus['add_time']
    #                         mage_chapters_file_list.append(form_chapters_file_list)
    #                         # 添加的数据
    #                         mage_chapters_file_list = json.dumps(mage_chapters_file_list,
    #                                                              ensure_ascii=False)
    #                         with open(mage_chapters_list_file_path, "w") as mage_chapters_f:
    #                             mage_chapters_f.write(mage_chapters_file_list)
    #
    #                         msg = "原站点%s更新至35 原书籍ID:%s更新的至书籍ID%s更新章节：%s" % (
    #                         form_source_id, neaten_value[0], mage_novel_id, mage_vals['_id'])
    #                         logging.info(msg)
    #                         # 更新的当前的书籍更新的总数
    #
    #                     else:
    #                         logging.info(form_name_msg + "章节的已经更新到最新")
    #
    #         update_novel_novels_neaten_sql = "update novel_novels_neaten set count=%s where novels_id=%s" % (
    #         form_chapters_count, neaten_value[0])
    #         cursor.execute(update_novel_novels_neaten_sql)
    #
    #
    #
    #     return 1
    """
       生成的子目录信息
       source_id 站点ID
       novels_id 书籍ID
       chapter_id 章节ID
       data 列表的字典
    """

    def add_novel_chapter_list(self, novels_id, chapter_id, data):

        path = "%s%s/%s/%s/" % (FILE_BASE, SOURCE_ID, novels_id, chapter_id)
        rec = os.path.exists(path)
        if rec == False:
            os.makedirs(path)

        list_data = []
        file_path = path + "list.json"
        with open(file_path, mode='w') as f:
            list_data.append(data)
            f.write(json.dumps(list_data, ensure_ascii=False))

        return 1



if __name__ == '__main__':
    book_id = sys.argv[1]
    new_book_id = sys.argv[2]
    r = novelMarge()
    r = r.repair_chapter(book_id,new_book_id)

