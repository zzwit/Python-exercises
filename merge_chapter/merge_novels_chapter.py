
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






# DB_HOST = '127.0.0.1'
# DB_USER = 'novelcode'
# DB_PWD = 'RvZ@7^yGR2waQNLJ'
# DB_NAME = 'novel_collect'
# DB_CHARSET = 'utf8'
# DB_PORT = 3306
#
#
# redis_config = {
#     "host": "61.160.196.39",
#     "port": 26890,
#     "db": 4,
#     "password": 'DH56ji2(3u^4',
# }


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



LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='./novel_mage_log.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)




m_mysql = pymysql.Connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PWD,
    port=DB_PORT,
    db=DB_NAME,
    charset=DB_CHARSET
)

redis_pool = redis.ConnectionPool(**redis_config)
my_redis = redis.Redis(connection_pool=redis_pool)

FILE_BASE = "/data/book/"

class novelMarge(object):

    def __init__(self):
        self.mysql =  m_mysql
        self.sredis = my_redis

    """
        获取所有的来源 地址数据 进行书籍匹配和合并 匹配到 90 以上
    """
    def get_update_novel_gather(self,novels_list):
        cursor = self.mysql.cursor()
        try:
            if novels_list:
                valss = novels_list
                # 获取的所有的相关的章节的最近更新的数量
                novel_chapters_sql = "select novels_id,count from  novel_chapters where novels_id IN(%s)" % (valss[4])

                cursor.execute(novel_chapters_sql)
                novel_chapters = cursor.fetchall()
                novel_chapters_list = {}
                for vals in novel_chapters:
                    novel_chapters_list[vals[0]] = vals[1]

                # 在获取的当前更新的长度
                novel_novels_neaten_sql = "select novels_id,source_id,count from  novel_novels_neaten where novels_id IN(%s)" % (valss[4])
                cursor.execute(novel_novels_neaten_sql)
                novel_novels_neaten = cursor.fetchall()

                # 获取的合并的书籍所有的章节内容
                newsPath = "%s%s/%s/chapter.json" % (FILE_BASE, 35, valss[1])

                # 合并的小说的书籍ID
                mage_novel_id = valss[1]
                with open(newsPath, 'r') as f:
                    mage_chapters_list = f.read()
                    mage_chapters_list = json.loads(mage_chapters_list)


                # 合并最新书籍章节数量
                mage_chapters_number = len(mage_chapters_list)
                novel_chapters_info = {}
                for index, neaten_vl in enumerate(novel_novels_neaten):
                    novel_chapters_info[neaten_vl[0]] = neaten_vl[1]

                # 来源 数据优先处理
                update_chapters_number = novel_chapters_list.get(valss[6])
                if update_chapters_number == None:
                    return 1

                if update_chapters_number > mage_chapters_number:
                    form_path_list = "%s%s/%s/chapter.json" % (
                        FILE_BASE, novel_chapters_info[valss[6]], valss[6])
                    form_recs = os.path.exists(form_path_list)
                    # 判断的文件是否存在

                    if form_recs == False:
                        msg = "书籍ID%s没有内容" % (valss[6])
                        logging.info(msg)
                        return 1

                    with open(form_path_list, 'r') as form_path_f:
                        form_path_chapters_list = form_path_f.read()
                        if form_path_chapters_list:
                            form_path_chapters_list = json.loads(form_path_chapters_list)
                        else:
                            return 1

                    if form_path_chapters_list[mage_chapters_number::]:
                        chapter_id = mage_chapters_number
                        for index, vs in enumerate(form_path_chapters_list[mage_chapters_number::]):
                            chapter_id += 1
                            chapters_list_data = {}
                            chapters_list_ads = {}
                            # 更新的新的章节
                            chapters_list_data['source_id'] = novel_chapters_info[valss[6]]
                            chapters_list_data['url'] = vs['url']
                            chapters_list_data['add_time'] = vs['add_time']
                            chapters_list_data['weight'] = 1

                            chapters_list_ads['_id'] = chapter_id
                            chapters_list_ads['name'] = vs['name']
                            chapters_list_ads['add_time'] = vs['add_time']
                            self.add_novel_chapter_list(valss[1], chapter_id, chapters_list_data)

                            mage_chapters_list.append(chapters_list_ads)
                            chapters_new_name = vs['name']
                        with open(newsPath, 'w') as f:
                            f.write(json.dumps(mage_chapters_list, ensure_ascii=False))

                        cursor = self.mysql.cursor()
                        chapters_update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        count = chapter_id
                        chapters_up_sql = 'update novel_chapters set is_sync=0,count=%s,new_name="%s",update_time="%s" where novels_id=%s' % (
                        count, chapters_new_name, chapters_update_time, mage_novel_id)
                        cursor.execute(chapters_up_sql)
                for neaten_value in novel_novels_neaten:

                    form_source_id = neaten_value[1]
                    form_name_msg = "来源的站点ID%s书籍ID:%s" % (form_source_id, neaten_value[0])

                    # 章节的那边的数据更新的章节
                    update_chapters_number = novel_chapters_list.get(neaten_value[0])

                    if update_chapters_number == None:
                        continue

                    # 当前的合并的书籍 更新的章节的数量
                    chapters_number = neaten_value[2]
                    # 比较的一下的书籍
                    if update_chapters_number > chapters_number:
                        formPath = "%s%s/%s/chapter.json" % (FILE_BASE, form_source_id, neaten_value[0])
                        form_path_recss = os.path.exists(formPath)
                        if form_path_recss == False:
                            msg = "书籍%s没有数据" % (neaten_value[0])
                            logging.info(msg)
                            continue

                        with open(formPath, 'r') as ff:
                            form_chapters_List = ff.read()

                        if form_chapters_List:
                            form_chapters_List = json.loads(form_chapters_List)
                            form_chapters_limit = form_chapters_List[chapters_number::]
                            form_chapters_count = len(form_chapters_List)
                            form_chapters_List = {}
                            # 新网站网站章节列表
                            for index, mage_vals in enumerate(mage_chapters_list):

                                minnumber = int(index - 5)
                                maxnumber = int(index + 6)
                                # 原网站的章节列表
                                for index, form_valus in enumerate(form_chapters_limit[minnumber:maxnumber]):
                                    rate = 0
                                    if mage_vals.get('name') and form_valus.get('name'):
                                        rate = difflib.SequenceMatcher(None, mage_vals['name'],
                                                                       form_valus['name']).quick_ratio()
                                        rate = int(round(rate, 2) * 100)
                                    if rate > 90:
                                        form_chapters_file_list = {}
                                        # 合并之后的数据
                                        mage_chapters_list_file_path = "%s%s/%s/%d/list.json" % (
                                            FILE_BASE, 35, mage_novel_id, mage_vals['_id'])
                                        mage_chapters_path_recss = os.path.exists(mage_chapters_list_file_path)

                                        if mage_chapters_path_recss == False:
                                            continue

                                        with open(mage_chapters_list_file_path, "r") as mage_chapters_f:
                                            mage_chapters_file_list = mage_chapters_f.read()
                                            mage_chapters_file_list = json.loads(mage_chapters_file_list)

                                        # 检查来源的是否存在的了
                                        is_status = 0
                                        for vs in mage_chapters_file_list:
                                            if vs['source_id'] == form_source_id:
                                                is_status = 1

                                        if is_status == 0:
                                            # 来源的站点的数据整合
                                            form_chapters_file_list['source_id'] = form_source_id
                                            form_chapters_file_list['url'] = form_valus['url']
                                            form_chapters_file_list['add_time'] = form_valus['add_time']
                                            mage_chapters_file_list.append(form_chapters_file_list)
                                            # 添加的数据
                                            mage_chapters_file_list = json.dumps(mage_chapters_file_list,
                                                                                 ensure_ascii=False)
                                            with open(mage_chapters_list_file_path, "w") as mage_chapters_f:
                                                mage_chapters_f.write(mage_chapters_file_list)

                                            msg = "原站点%s更新至35 原书籍ID:%s更新的至书籍ID%s更新的章节：%s" %(form_source_id,neaten_value.get(0),mage_novel_id,mage_vals['_id'])
                                            logging.info(msg)
                                            # 更新的当前的书籍更新的总数

                                        else:
                                            logging.info(form_name_msg+"章节的已经更新到最新")

                            update_novel_novels_neaten_sql = "update novel_novels_neaten set count=%s where novels_id=%s" % (form_chapters_count, neaten_value[0])
                            cursor.execute(update_novel_novels_neaten_sql)
                            self.mysql.commit()
                    else:

                        logging.info(form_name_msg+"暂无新的章节的更新信息")
            else:
                logging.info("暂无更新")
        except Exception as e:
            cursor.close()
            logging.error(novels_list+e)
        cursor.close()


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





"""
 执行的任务的
"""
def run_task(s_id,novels_list):
    novel_mage_model = novelMarge()
    novel_mage_model.get_update_novel_gather(novels_list)
    # print('Run task %s (%s)...' % (s_id,os.getpid()))
    # start = time.time()
    # time.sleep(random.random() * 3)
    # end = time.time()
    # print('Task %s runs %0.2f seconds.' % (s_id, (end - start)))



if __name__=='__main__':


    print('Parent process %s.' % os.getpid())
    # 开启 进程
    p = Pool(8)
    # 多少任务
    cursor_model = m_mysql.cursor()
    sql = "select * from novel_novels_gather where new_novels_id>0 order by  weight asc"
    cursor_model.execute(sql)
    novels_list = cursor_model.fetchall()
    cursor_model.close()
    logging.info("程序开始......")
    #获取任务之后进入队列中
    for index,novelss in enumerate(novels_list):
        ss = "在处理新书籍ID %s 更新数量 %s" %(novelss[1],index)
        logging.info(ss)
        p.apply_async(run_task, args=(index,novelss,))

    p.close()
    p.join()
    inlog = "数据处理完成，总共处理%s 条数据" % (len(novels_list))
    print(inlog)
    logging.info(inlog)



