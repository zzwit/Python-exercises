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

from merge_chapter.merge_mongo import *


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

    def main(self):
        # 第四步 把多个章节内容合并成list.json 格式
        self.add_new_novels()


        #第三步 合并出新的书籍 创建新书籍
        #self.add_new_novels()
        # 第二部要合并数据缓存
        #self.get_novel_neaten()
        # 第一步 项目执行次数
        #self.novel_neaten()

    """
        合并新书必须拷贝章节内容
    """
    def add_new_novels(self):
        # if self.sredis.get("add_new_novels"):
        #     print("程序在进行中。请等待。。。")
        #     exit()
        # else:
        #     self.sredis.set("add_new_novels", 1)

        lg = 0
        page_index = 0
        limit = 1000
        cursor = self.mysql.cursor()

        try:
            while lg == 0:
                page = page_index * limit
                sql = "select id,novels_ids from `novel_novels_gather` where new_novels_id = 0 limit %d,%d" % (page, limit)
                cursor.execute(sql)
                novels_ids = cursor.fetchall()
                lencut = len(novels_ids)

                if lencut == 0:
                    lg = 1
                    cursor.close()
                    self.sredis.delete("add_new_novels")
                    exit()
                else:
                    if novels_ids:
                        page_index +=1
                        for val in novels_ids:

                            #获取小说的第一个站点
                            novel_id_sql = "select novels_id,source_id from novel_novels_neaten where source_id in(62,3,93,15,4) and novels_id in(%s) limit 1" %(val[1])
                            cursor.execute(novel_id_sql)
                            novels_ids = cursor.fetchone()

                            if novels_ids == None:
                                recs = val[1].split(',')
                                novels_ids = recs[0]
                            else:
                                novels_ids = novels_ids[0]

                            novel_novels_sql = "select * from novel_novels where id=%s " %(novels_ids)
                            cursor.execute(novel_novels_sql)
                            novels_info = cursor.fetchone()

                            if novels_info:
                                name_author_que_str = "%s$c%s" %(novels_info[1],novels_info[6])

                                name_author_que = hashlib.md5(name_author_que_str.encode()).hexdigest()

                                #查看是否填写过新书
                                restar_sql = "select id from novel_novels where name_author_que='%s'" %(name_author_que)
                                cursor.execute(restar_sql)
                                recs  = cursor.fetchone()

                                if recs == None:
                                    #获取当前站点小说的章节列表
                                    chapter_list = self.get_chapter_list(novels_info[16],novels_info[0])

                                    if chapter_list:
                                        #添加的新书
                                        instr = 'insert into novel_novels (name,url,corver_url,corver_path,info,author,weight,read_num,word_num,is_action,has_content,gender,add_time,update_time,types_id,source_id,is_show,name_author_que,form_novels_id) VALUES'
                                        instr += '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                                        cursor.execute(instr,args= (novels_info[1],novels_info[2],novels_info[3],novels_info[4],novels_info[5],novels_info[6],novels_info[7],novels_info[8],novels_info[9],novels_info[10],novels_info[11],novels_info[12],novels_info[13],novels_info[14],novels_info[15],35,novels_info[17],name_author_que,novels_info[0]))
                                        self.mysql.commit()
                                        new_novel_id = cursor.lastrowid

                                    else:
                                        new_novel_id = False

                                    if new_novel_id:
                                        novel_chapters_path = "%s/%s/%s" % (SOURCE_ID, new_novel_id, "chapter.json")
                                        #数据写入的到Mongo表里
                                        new_chapter_list = []
                                        #添加的列表的信息整合
                                        add_chapter = {}

                                        if chapter_list:
                                            # print(type(chapter_list))
                                            # exit()
                                            for chapter in chapter_list:
                                                source_list = []
                                                #TODO 明天接着写
                                                new_chapters = {}
                                                chapters_list_data = {}

                                                new_chapters['_id'] = chapter['_id']
                                                new_chapters['name'] = chapter['name']
                                                new_chapters['add_time'] = chapter['add_time']

                                                chapters_list_data['source_id'] = novels_info[16]
                                                chapters_list_data['url'] = chapter['url']
                                                chapters_list_data['add_time'] = chapter['add_time']
                                                chapters_list_data['weight'] = 1
                                                source_list.append(chapters_list_data)
                                                new_chapters['source_list'] = source_list
                                                #整理
                                                new_chapter_list.append(new_chapters)

                                                #new_chapter[str(chapter['_id'])] = new_chapters
                                                #最新章节
                                                new_chapter_name = new_chapters['name']


                                            upcount = len(new_chapter_list)

                                            add_chapter['_id'] = new_novel_id
                                            add_chapter['chapters'] = new_chapter_list


                                            #生产的章节的列表
                                            self.add_novel_chapter_list(new_novel_id,add_chapter)
                                            update_sql = "update novel_novels_gather set new_novels_id=%d,form_novels_id=%d where id=%d" %(new_novel_id,novels_info[0],val[0])
                                            cursor.execute(update_sql)
                                            neaten_update_sql = "update novel_novels_neaten set count=%d where novels_id=%d" %(upcount,novels_info[0])
                                            cursor.execute(neaten_update_sql)
                                            self.mysql.commit()
                                            #添加的更新的最新章节的信息
                                            add_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                                            insert_sql = 'insert into novel_chapters (new_name,path,novels_id,update_time,add_time,count,is_sync) VALUES ("%s","%s",%d,"%s","%s",%d,0)' %(new_chapter_name,novel_chapters_path,new_novel_id,add_time,add_time,upcount)
                                            cursor.execute(insert_sql)
                                            self.mysql.commit()
                                            print("新增成功")
                                        else:
                                            print("暂无数据")
                                    else:
                                        print("新增数据失败")
                                else:
                                    print("已经存在")
                            else:
                                print("不存在")
                    else:
                        print("没有相似小说")
        except Exception as e:
            print(e)
            self.sredis.delete("add_new_novels")
            exit()

        self.sredis.delete("add_new_novels")
        exit()


    """
        获取的章节列表
    """
    def get_chapter_list(self,source_id,novels_id):

        if source_id == 35:
            pass
        else:
            source_id_y = source_id%10
            novels_id_y = novels_id%10

            source_name = "source_%s"%(source_id_y)
            collection_name = "novel_%s"%(novels_id_y)

            m_mongo = merge_mongo(source_name,collection_name)
            datas = m_mongo.m_chapters_list(novels_id)
            # print(datas)
            # exit()
            new_datas = []
            #格式化数据
            if datas:
                # for index in datas:
                #     new_datas.append(datas[index])
                # return  new_datas
                return datas
            else:
                return datas
    """
     生成的子目录信息
     source_id 站点ID
     novels_id 书籍ID
     chapter_id 章节ID
     data 列表的字典
    """
    def add_novel_chapter_list(self,novels_id,data):
        #将数据分成10份 FALSE
        novel_cel = novels_id % 10
        source_name = "source_%s" % (SOURCE_ID)
        collection_name = "novel_%s" % (novel_cel)
        m_mongo = merge_mongo(source_name, collection_name)
        status = m_mongo.m_chapters_count(novels_id)

        if status :
            updata = data.get("chapters")
            if updata:
                rec = m_mongo.m_replace_chapters(novels_id,updata)
                return rec
            else:
                return False
        else:
            rec = m_mongo.m_insert(data)
            return rec


if __name__ == '__main__':
    #sys_name = sys.argv[1]
    r = novelMarge()
    r.add_new_novels()
    # if sys_name == "update_novel":
    #     r.get_update_novel_gather()
    # elif sys_name == "add_new_novels":
    #     r.add_new_novels()
    # elif sys_name == "mage_novel_neaten":
    #     r.get_novel_neaten()
    # elif sys_name == "novel_neaten":
    #     r.novel_neaten()

        # 第四步 把多个章节内容合并成list.json 格式
        # self.get_update_novel_gather()
        # 第三步 合并出新的书籍 创建新书籍
        # self.add_new_novels()
        # 第二部要合并数据缓存
        # self.get_novel_neaten()
        # 第一步 项目执行次数
        # self.novel_neaten()

