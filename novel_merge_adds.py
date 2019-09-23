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
from pymongo import MongoClient



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

MG_HOST = "221.195.1.228"
MG_PORT = 27017
MG_USER = "admin"
MG_PWD  = "admin@1234"
host = '221.195.1.228'
client = MongoClient(host, 27017,username="admin",password="admin@1234")


FILE_BASE = "/data/book/"

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

        mg_client = MongoClient(MG_HOST, MG_PORT, username=MG_USER, password=MG_PWD)

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
                sql = "select id,novels_ids from `novel_novels_gather` where new_novels_id=0 limit %d,%d" % (page, limit)

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
                            novel_id_sql = "select novels_id from novel_novels_neaten where source_id in(62,3,93,15,4) and novels_id in(%s) limit 1" %(val[1])
                            cursor.execute(novel_id_sql)
                            novels_ids = cursor.fetchone()

                            if novels_ids == None:
                                recs = val[1].split(',')
                                novels_ids = recs[0]
                            else:
                                novels_ids = novels_ids[0]


                            novel_novels_sql = "select * from novel_novels where id=%d " %(novels_ids)
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

                                    fromPath = "%s%s/%s/chapter.json" % (FILE_BASE, novels_info[16], novels_info[0])

                                    if os.path.isfile(fromPath) == False:
                                        print("没有目录")
                                    else:
                                        
                                        with open(fromPath, 'r') as f:
                                                flist = f.read()
                                                if flist:
                                                    print("新版分析")
                                                    chapter_list = json.loads(flist)

                                                else:
                                                    chapter_list = ""
                                                    print("目录没有内容")

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
                                            #数据写入的到Mongo表里

                                            newsPath = "%s%s/%s/" % (FILE_BASE, 35,new_novel_id)
                                            newsFilePath = newsPath+"chapter.json"
                                            novel_chapters_path = "%s/%s/%s" %(35,new_novel_id,"chapter.json")

                                            #TODO 添加novel_chapters 表数
                                            #判断文件是否存在
                                            is_file_path = os.path.isfile(fromPath)
                                            if is_file_path == True:
                                                os.makedirs(newsPath)

                                                #json.dumps()
                                                new_chapter = []
                                                if chapter_list:
                                                    for chapter in chapter_list:
                                                        new_chapters = {}
                                                        chapters_list_data = {}
                                                        new_chapters['_id'] = chapter['_id']
                                                        new_chapters['name'] = chapter['name']
                                                        new_chapters['add_time'] = chapter['add_time']
                                                        chapters_list_data['source_id'] = novels_info[16]
                                                        chapters_list_data['url'] = chapter['url']
                                                        chapters_list_data['add_time'] = chapter['add_time']
                                                        chapters_list_data['weight'] = 1

                                                        self.add_novel_chapter_list(new_novel_id, chapter['_id'],chapters_list_data)
                                                        new_chapter.append(new_chapters)
                                                        #最新章节
                                                        new_chapter_name = new_chapters['name']

                                                    upcount = len(new_chapter)

                                                    with open(newsFilePath, 'w') as f:
                                                         f.write(json.dumps(new_chapter,ensure_ascii=False))
                                                    #修改合并表信息
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
                                                else:
                                                    print("暂无数据")
                                                print("生成成功")
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
     生成的子目录信息
     source_id 站点ID
     novels_id 书籍ID
     chapter_id 章节ID
     data 列表的字典
    """
    def add_novel_chapter_list(self,novels_id,chapter_id,data):

        path = "%s%s/%s/%s/" % (FILE_BASE, 35, novels_id,chapter_id)
        rec = os.path.exists(path)
        file_path = path + "list.json"
        if rec == False:
            os.makedirs(path)
            list_data = []
        else:
            with open(file_path,'r') as fr:
                list_data = json.loads(fr.read())

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

