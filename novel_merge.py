"""
 合并小说的内容
"""
import pymysql.cursors
import redis
import hashlib
import shutil
import os
import json
import time
import difflib
import sys





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



FILE_BASE = "/data1/book/"

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
        self.get_update_novel_gather()


        #第三步 合并出新的书籍 创建新书籍
        #self.add_new_novels()
        # 第二部要合并数据缓存
        #self.get_novel_neaten()
        # 第一步 项目执行次数
        #self.novel_neaten()

    """
        获取所有的来源 地址数据 进行书籍匹配和合并 匹配到 90 以上
    """
    def get_update_novel_gather(self):
        if self.sredis.get("update_novel"):
            print("程序在进行中。请等待。。。")
            exit()
        else:
            self.sredis.set("update_novel", 1)

        sql = "select * from novel_novels_gather where new_novels_id>0 order by  weight asc "
        cursor = self.mysql.cursor()
        cursor.execute(sql)
        novels_list = cursor.fetchall()
        if novels_list:
            for valss in novels_list:
                #获取的所有的相关的章节的最近更新的数量
                novel_chapters_sql = "select novels_id,count from  novel_chapters where novels_id IN(%s)" %(valss[4])
                cursor.execute(novel_chapters_sql)
                novel_chapters = cursor.fetchall()

                novel_chapters_list = {}

                for vals in novel_chapters:
                    novel_chapters_list[vals[0]] = vals[1]


                #在获取的当前更新的长度
                novel_novels_neaten_sql = "select novels_id,source_id,count from  novel_novels_neaten where novels_id IN(%s)" %(valss[4])
                cursor.execute(novel_novels_neaten_sql)
                novel_novels_neaten = cursor.fetchall()

                # 获取的合并的书籍所有的章节内容
                newsPath = "%s%s/%s/chapter.json" % (FILE_BASE, 35, valss[1])

                #合并的小说的书籍ID
                mage_novel_id = valss[1]
                with open(newsPath,'r') as f:
                    mage_chapters_list = f.read()
                    mage_chapters_list = json.loads(mage_chapters_list)
                #合并最新书籍章节数量
                mage_chapters_number = len(mage_chapters_list)
                novel_chapters_info = {}
                for neaten_vl in novel_novels_neaten:
                    novel_chapters_info[neaten_vl[0]] = neaten_vl[1]

                print(novel_chapters_list)
                print(valss[6])
                #来源 数据优先处理
                update_chapters_number = novel_chapters_list[valss[6]]

                if update_chapters_number > mage_chapters_number:
                    form_path_list = "%s%s/%s/chapter.json" % (FILE_BASE, novel_chapters_info[valss[6]], valss[6])
                    with open(form_path_list, 'r') as form_path_f:
                        form_path_chapters_list = form_path_f.read()
                        form_path_chapters_list = json.loads(form_path_chapters_list)

                    if form_path_chapters_list[mage_chapters_number::] :
                        chapter_id = mage_chapters_number
                        for vs in form_path_chapters_list[mage_chapters_number::]:
                            chapter_id +=1
                            chapters_list_data = {}
                            chapters_list_ads = {}
                             # 更新的新的章节
                            chapters_list_data['source_id'] =  novel_chapters_info[valss[6]]
                            chapters_list_data['url'] = vs['url']
                            chapters_list_data['add_time'] = vs['add_time']

                            chapters_list_ads['_id'] = chapter_id
                            chapters_list_ads['name'] = vs['name']
                            chapters_list_ads['add_time'] = vs['add_time']
                            self.add_novel_chapter_list(valss[1], chapter_id, chapters_list_data)
                            mage_chapters_list.append(chapters_list_ads)

                        with open(newsPath, 'w') as f:
                            f.write(json.dumps(mage_chapters_list,ensure_ascii=False))
                for neaten_value in  novel_novels_neaten:
                    form_source_id = neaten_value[1]
                    form_name_msg = "来源的站点ID%s书籍ID:%s" % (form_source_id,neaten_value[0])
                    #章节的那边的数据更新的章节
                    update_chapters_number = novel_chapters_list[neaten_value[0]]
                    #当前的合并的书籍 更新的章节的数量
                    chapters_number = neaten_value[2]
                    # 比较的一下的书籍
                    if update_chapters_number > chapters_number:

                        formPath = "%s%s/%s/chapter.json" % (FILE_BASE, form_source_id, neaten_value[0])
                        with open(formPath, 'r') as ff:
                            form_chapters_List = ff.read()

                        if form_chapters_List:
                            form_chapters_List = json.loads(form_chapters_List)
                            form_chapters_limit = form_chapters_List[chapters_number::]
                            form_chapters_count = len(form_chapters_List)
                            #新网站网站章节列表
                            for mage_vals in mage_chapters_list:
                                 # 原网站的章节列表
                                 for form_valus in form_chapters_limit:
                                     #print(mage_vals['name'])

                                     if mage_vals.get('name') and form_valus.get('name'):
                                         rate = difflib.SequenceMatcher(None, mage_vals['name'],form_valus['name']).quick_ratio()
                                         rate = int(round(rate,2) * 100)
                                     else:
                                         rate = 0
                                     if rate > 95:
                                         form_chapters_file_list = {}
                                         #合并之后的数据
                                         mage_chapters_list_file_path = "%s%s/%s/%d/list.json" % (FILE_BASE, 35,mage_novel_id,mage_vals['_id'])
                                         with open(mage_chapters_list_file_path,"r") as mage_chapters_f:
                                             mage_chapters_file_list = mage_chapters_f.read()
                                             mage_chapters_file_list = json.loads(mage_chapters_file_list)

                                         #检查来源的是否存在的了
                                         is_status = 0
                                         for vs in mage_chapters_file_list:
                                             if vs['source_id'] == form_source_id:
                                                 is_status = 1
                                         if is_status == 0:
                                             #来源的站点的数据整合
                                             form_chapters_file_list['source_id'] = form_source_id
                                             form_chapters_file_list['url'] = form_valus['url']
                                             form_chapters_file_list['add_time'] = form_valus['add_time']
                                             mage_chapters_file_list.append(form_chapters_file_list)
                                             #添加的数据
                                             mage_chapters_file_list = json.dumps(mage_chapters_file_list)
                                             with open(mage_chapters_list_file_path, "w") as mage_chapters_f:
                                                 mage_chapters_f.write(mage_chapters_file_list)

                                             print("原站点",form_source_id,'更新至35',"原书籍ID:",neaten_value[0],"更新的至书籍ID:",mage_novel_id,"更新的章节：",mage_vals['_id'])
                                         else:
                                             print(form_name_msg,"章节的已经更新到最新")
                            #更新的当前的书籍更新的总数
                            update_novel_novels_neaten_sql = "update novel_novels_neaten set count=%d where novels_id=%d" %(form_chapters_count,neaten_value[0])
                            cursor.execute(update_novel_novels_neaten_sql)
                            self.mysql.commit()
                        else:
                            continue
                    else:
                        print(form_name_msg,"暂无新的章节的更新信息")
        else:
            print("暂无更新")

        self.sredis.delete("update_novel")


    """
        合并新书必须拷贝章节内容
    """
    def add_new_novels(self):
        if self.sredis.get("add_new_novels"):
            print("程序在进行中。请等待。。。")
            exit()
        else:
            self.sredis.set("add_new_novels", 1)

        lg = 0
        page_index = 0
        limit = 1
        cursor = self.mysql.cursor()
        try:
            while lg == 0:
                page = page_index * limit
                sql = "select id,novels_ids from `novel_novels_gather` where new_novels_id =0 ORDER BY  weight asc limit %d,%d" % (page, limit)
                cursor.execute(sql)
                novels_ids = cursor.fetchall()
                lencut = len(novels_ids)
                lg = 1
                if lencut == 0:
                    lg = 1
                    cursor.close()
                else:
                    if novels_ids:
                        for val in novels_ids:
                            #获取小说的第一个站点
                            novel_id_sql = "select novels_id from novel_novels_neaten where source_id=3 and novels_id in(%s)" %(val[1])
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
                                        #添加的新书
                                        instr = 'insert into novel_novels (name,url,corver_url,corver_path,info,author,weight,read_num,word_num,is_action,has_content,gender,add_time,update_time,types_id,source_id,is_show,name_author_que,form_novels_id) VALUES'
                                        instr += '("%s","%s","%s","%s","%s","%s",%d,%d,%d,%d,%d,%d,"%s","%s",%d,%d,%d,"%s",%d)' %(novels_info[1],novels_info[2],novels_info[3],novels_info[4],novels_info[5],novels_info[6],novels_info[7],novels_info[8],novels_info[9],novels_info[10],novels_info[11],novels_info[12],novels_info[13],novels_info[14],novels_info[15],35,novels_info[17],name_author_que,novels_info[0])
                                        cursor.execute(instr)
                                        self.mysql.commit()
                                        new_novel_id = cursor.lastrowid

                                        if new_novel_id:

                                            newsPath = "%s%s/%s/" % (FILE_BASE, 35,new_novel_id)
                                            newsFilePath = newsPath+"chapter.json"
                                            novel_chapters_path = "%s/%s/%s" %(35,new_novel_id,"chapter.json")
                                            #TODO 添加novel_chapters 表数
                                            #判断文件是否存在
                                            is_file_path = os.path.isfile(fromPath)
                                            if is_file_path == True:
                                                os.makedirs(newsPath)
                                                with open(fromPath, 'r') as f:
                                                    chapter_list = json.loads(f.read())
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
                                                    insert_sql = 'insert into novel_chapters (new_name,path,novels_id,update_time,add_time,count,is_sync) VALUES ("%s","%s",%d,"%s","%s",%d,1)' %(new_chapter_name,novel_chapters_path,new_novel_id,add_time,add_time,upcount)
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
        except:
            self.sredis.delete("add_new_novels")

        self.sredis.delete("add_new_novels")

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
            print(list_data)
        return 1

    """
        获取语新增数据
    """
    def get_novel_neaten(self):
        if self.sredis.get("mage_novel_neaten"):
            print("程序在进行中。请等待。。。")
            exit()
        else:
            self.sredis.set("mage_novel_neaten",1)
        #分页预加载
        lg = 0
        page_index = 0
        limit = 100
        cursor = self.mysql.cursor()
        while lg == 0:
            page = page_index * limit
            sql  = "select name_author,group_concat(novels_id) as novels_ids,max(weight) as weight from `novel_novels_neaten`  GROUP BY  name_author HAVING  count(1) > 1 ORDER BY  max(weight) asc ,count(1)  desc limit %d,%d" % (page, limit)
            cursor.execute(sql)
            novel_list = cursor.fetchall()
            lencut =  len(novel_list)
            if lencut == 0 :
                lg = 1
                cursor.close()
            else:
                if novel_list:
                    instr = 'insert ignore into novel_novels_gather (name_author,name_author_que,novels_ids,weight) values'
                    for index,val in enumerate(novel_list,1):
                        name_authot_que = hashlib.md5(val[0].encode()).hexdigest()
                        if index == lencut:
                            str = '("%s","%s","%s",%d)' % (val[0], name_authot_que, val[1],val[2])
                        else:
                            str = '("%s","%s","%s",%d),' % (val[0], name_authot_que, val[1],val[2])

                        instr += str
                    cursor.execute(instr)
                    self.mysql.commit()
                    print("更新第",page_index,'页')
                page_index += 1
        self.sredis.delete("mage_novel_neaten")
    def novel_neaten(self):
        if self.sredis.get("novel_neaten"):
            print("程序在使用中。。。。")
            exit()
        else:
            self.sredis.set("novel_neaten",1)
        #数据整理
        cursor = self.mysql.cursor()
        #查询的信息的最大的数据
        maxsql = "select novels_id from novel_novels_neaten order by novels_id desc limit 1"
        cursor.execute(maxsql)
        neaten_info = cursor.fetchone()
        if neaten_info :
            neaten_novel_id = neaten_info[0]
            countsql = "select count(1) from novel_novels where id>%d limit 1" % (neaten_novel_id)
        else:
            neaten_novel_id = ''
            countsql = "select count(1) from novel_novels limit  1"

        cursor.execute(countsql)
        scount = cursor.fetchone()
        if scount:
            scount = scount[0]
            print(scount)
            pagecont = int(scount/1000)
            pagecont = pagecont+1
            for i in range(pagecont):
                self.curm_sql(neaten_novel_id,i);
                print("更新到分页",i)
            cursor.close()
        self.sredis.delete("novel_neaten");
        print("数据整理完成")
    def curm_sql(self,neaten_novel_id=0,page=0,limit = 1000):
        page = page * 1000
        if neaten_novel_id:
            sql = "select id,source_id,name,url,author,weight from novel_novels where id>%d limit %d,%d" % (neaten_novel_id,page,limit)
        else:
            sql = "select id,source_id,name,url,author,weight from novel_novels limit %d,%d" %(page,limit)
        cursor = self.mysql.cursor()
        cursor.execute(sql)
        novel_list = cursor.fetchall()

        if novel_list:
            instr = 'insert into novel_novels_neaten (novels_id,url,source_id,name_author,count,weight) values'
            #当前的书籍总数
            novel_list_cont = len(novel_list)
            for index,val in enumerate(novel_list,1):
                name_author = "%s$c%s" %(val[2],val[4])

                if index == novel_list_cont:
                    str = '(%d,"%s",%d,"%s",%d,%d)' % (val[0], val[3], val[1], name_author, 0, val[5])
                else:
                    str = '(%d,"%s",%d,"%s",%d,%d),' % (val[0], val[3], val[1], name_author, 0, val[5])

                instr +=str

            cursor.execute(instr)
            self.mysql.commit()
            cursor.close()
if __name__ == '__main__':
    sys_name = sys.argv[1]
    r = novelMarge()

    if sys_name == "update_novel":
        r.get_update_novel_gather()
    elif sys_name == "add_new_novels":
        r.add_new_novels()
    elif sys_name == "mage_novel_neaten":
        r.get_novel_neaten()
    elif sys_name == "novel_neaten":
        r.novel_neaten()

        # 第四步 把多个章节内容合并成list.json 格式
        # self.get_update_novel_gather()
        # 第三步 合并出新的书籍 创建新书籍
        # self.add_new_novels()
        # 第二部要合并数据缓存
        # self.get_novel_neaten()
        # 第一步 项目执行次数
        # self.novel_neaten()

