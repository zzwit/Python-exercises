import redis
import time
import pymysql.cursors
import difflib
import logging
import json
import os

from merge_mongo import *

"""
Redis 实现的订阅  点阅者

"""

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='/data/www/novel_script/logs/update_novel_mage_'+time.strftime("%Y-%m-%d", time.localtime())+'.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)

num_str_start_symbol = ['一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十']
more_num_str_symbol = ['零', '一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十', '百', '千', '万', '亿']
common_used_numerals_tmp = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
                            '十': 10, '百': 100, '千': 1000, '万': 10000, '亿': 100000000}
common_used_numerals = {}
for key in common_used_numerals_tmp:
    common_used_numerals[key] = common_used_numerals_tmp[key]


# 本地

# DB_HOST = '127.0.0.1'
# DB_USER = 'root'
# DB_PWD = 'root'
# DB_NAME = 'novel_collect'
# DB_CHARSET = 'utf8'
# DB_PORT = 3306
#
# REDIS_HOST = "127.0.0.1"
# REDIS_PORT = 6379
# REDIS_DB   = 3
# REDIS_PWD  = ''




# 线上
#Mysql
DB_HOST = '192.168.0.18'
DB_USER = 'novelcode'
DB_PWD = 'RvZ@7^yGR2waQNLJ'
DB_NAME = 'novel_collect'
DB_CHARSET = 'utf8'
DB_PORT = 3306
# Redis
REDIS_HOST = "192.168.0.39"
REDIS_PORT = 26890
REDIS_DB = 3
REDIS_PWD = 'DH56ji2(3u^4'

#数据库链接
m_mysql = pymysql.Connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PWD,
    port=DB_PORT,
    db=DB_NAME,
    charset=DB_CHARSET,
    autocommit = True

)

#Redis 链接
my_redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PWD)

FILE_BASE = "/data/book/"

SOURCE_ID = 35
#过滤器
class FilterTag(object):

    def __init__(self, separator=u' '):
        self.separator = separator

    def __call__(self, values):
        if values:
            valus_name = values['name'].strip()
            novelMargeModel = novelMarge()
            valus_name = novelMargeModel.changeChineseNumToArab(valus_name)
            filter_rate = difflib.SequenceMatcher(None, self.separator,valus_name).quick_ratio()
            filter_rate = int(round(filter_rate, 2) * 100)
            if filter_rate < 90:
                return 0
            else:
                return 1
        else:
            return 0

class novelMarge(object):

    def __init__(self):
        self.mysql =  m_mysql
        self.sredis = my_redis

    """
        获取所有的来源 地址数据 进行书籍匹配和合并 匹配到 90 以上
    """
    def update_novel_gather(self,novels_list):

        cursor = self.mysql.cursor()
        #try:
        if novels_list:

            # 合并的小说的书籍ID
            mage_novel_id = novels_list['new_novels_id']
            #当前要更新的书籍ID 如果有多个 可以是 1,2 这个样式
            up_novel_id =  novels_list['up_novels_id']
            #来源的书籍ID列表
            form_novels_id = novels_list["form_novels_id"]

            name_author_string = novels_list['name_author']
            mage_novel_name = name_author_string.split('$c')[0]


            # 获取的所有的相关的章节的最近更新的数量
            novel_chapters_sql = "select novels_id,count from  novel_chapters where novels_id = %s" % (up_novel_id)
            cursor.execute(novel_chapters_sql)
            novel_chapters = cursor.fetchall()
            novel_chapters_list = {}

            #计算出最更新最大的
            update_novels_id = 0
            for index,vals in enumerate(novel_chapters):
                if(index ==0):
                    update_novels_id = vals[0]
                    update_novel_max_number = vals[1]
                novel_chapters_list[vals[0]] = vals[1]


            # 在获取的当前更新的长度
            novel_novels_neaten_sql = "select novels_id,source_id,count from  novel_novels_neaten where novels_id = %s" % (up_novel_id)
            cursor.execute(novel_novels_neaten_sql)
            novel_novels_neaten = cursor.fetchall()


            # 获取的合并的书籍所有的章节内容
            newsPath = "%s%s/%s/chapter.json" % (FILE_BASE, SOURCE_ID, mage_novel_id)
            # with open(newsPath, 'r') as f:
            #     mage_chapters_list = f.read()
            #     mage_chapters_list = json.loads(mage_chapters_list)

            mage_chapters_list = self.get_chapter_list(SOURCE_ID,mage_novel_id)

            if not mage_chapters_list:
                logging.info("没有内容")
                return True;
            # 合并最新书籍章节数量
            mage_chapters_number = len(mage_chapters_list)
            novel_chapters_info = {}
            for index, neaten_vl in enumerate(novel_novels_neaten):
                novel_chapters_info[neaten_vl[0]] = neaten_vl[1]


            # 来源 数据优先处理
            update_chapters_number = novel_chapters_list.get(form_novels_id)


            # 先判断是否有最新的章节合并
            if update_novels_id != form_novels_id:
                #不能等于的主要来源ID查看的一下更新数量是否相等
                if update_novel_max_number == update_chapters_number:
                    update_novels_id = form_novels_id
                else:
                    update_chapters_number = update_novel_max_number

            #更新数量大于的执行 合并书籍最新章节
            #if update_chapters_number > mage_chapters_number:
                #如果为空！ 计算更新最多的小说来源
                #这个是小说的最新的 update_novels_id 小说ID
            logging.info("合并新目录")
            #测试别的程序 TODO 这一块速度很慢这个要检查一下

            mage_chapters_list = self.chack_new_chapter(update_chapters_number,mage_chapters_number,update_novels_id,novel_chapters_info,mage_chapters_list,newsPath,mage_novel_id,mage_novel_name)

            for neaten_value in novel_novels_neaten:

                #初始化更新的章节的数量
                update_chapters_number = 0
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

                    form_chapters_List = self.get_chapter_list(form_source_id,neaten_value[0])

                    if form_chapters_List:
                        form_chapters_limit = form_chapters_List[chapters_number::]
                        form_chapters_count = len(form_chapters_List)

                        form_chapters_List = {}
                        # 新网站网站章节列表
                        for index, mage_vals in enumerate(mage_chapters_list):

                            minnumber = int(index - 10) if int(index - 10) > 0 else 0
                            maxnumber = int(index + 6)

                            # 原网站的章节列表
                            for index, form_valus in enumerate(form_chapters_limit[minnumber:maxnumber]):
                                rate = 0
                                if mage_vals.get('name') and form_valus.get('name'):
                                    mage_vals_name = self.changeChineseNumToArab(mage_vals['name'])
                                    form_valus_name = self.changeChineseNumToArab(form_valus['name'])
                                    rate = difflib.SequenceMatcher(None, mage_vals_name, form_valus_name).quick_ratio()
                                    rate = int(round(rate, 2) * 100)
                                if rate > 90:
                                    form_chapters_file_list = {}
                                    #当前合并站来源信息
                                    mage_source_list = mage_vals['source_list']

                                    # 检查来源的是否存在的了
                                    is_status = 0
                                    for vs in mage_source_list:
                                        if vs['source_id'] == form_source_id:
                                            is_status = 1


                                    if is_status == 0:
                                        # 来源的站点的数据整合
                                        form_chapters_file_list['source_id'] = form_source_id
                                        form_chapters_file_list['url'] = form_valus['url']
                                        form_chapters_file_list['add_time'] = form_valus['add_time']
                                        form_chapters_file_list['weight'] = 1
                                        mage_source_list.append(form_chapters_file_list)
                                        # 添加的数据
                                        # update_mage_source_list = {}
                                        # update_mage_source_info = {}
                                        # update_mage_source_info["_id"] = mage_vals['_id']
                                        # update_mage_source_info["name"] = mage_vals['name']
                                        # update_mage_source_info["add_time"] = mage_vals['add_time']
                                        # update_mage_source_info["source_list"] = mage_source_list
                                        #
                                        # mage_chapters_id = str(mage_vals['_id'])
                                        # # print(update_mage_source_info)
                                        # # exit()
                                        # update_mage_source_list = update_mage_source_info
                                        mage_chapters_id = str(mage_vals['_id'])
                                        self.update_novel_source_list(mage_novel_id,mage_chapters_id,mage_source_list)
                                        msg = "原站点%s更新至35 原书籍ID:%s更新的至书籍ID%s更新章节：%s" %(form_source_id,neaten_value[0],mage_novel_id,mage_vals['_id'])
                                        logging.info(msg)
                                        # 更新的当前的书籍更新的总数
                                    else:
                                        logging.info(form_name_msg+"章节的已经更新到最新")

                        update_novel_novels_neaten_sql = "update novel_novels_neaten set count=%s where novels_id=%s" % (form_chapters_count, neaten_value[0])
                        cursor.execute(update_novel_novels_neaten_sql)
                else:

                    logging.info(form_name_msg+"暂无新的章节的更新信息")
        else:
            logging.info("暂无更新")
        # except Exception as e:
        #     cursor.close()
        #     novels_list_mage = str(novels_list) + e
        #     logging.error()


        logging.info(form_name_msg + "更新结束")
        cursor.close()


    """
        查看是否有新的章节
        update_chapters_number 小说最近更新章节数量
        mage_chapters_number   合并小说的更新章节数量
        update_novels_id         来源小说ID
        mage_novel_id          合并小说ID
    """
    def chack_new_chapter(self,update_chapters_number,mage_chapters_number,update_novels_id,novel_chapters_info,mage_chapters_list,newsPath,mage_novel_id,mage_novel_name):
        #获取来源的目录
        form_path_chapters_list = self.get_chapter_list(novel_chapters_info[update_novels_id],update_novels_id)

        if not form_path_chapters_list:
            return  mage_chapters_list

        mage_name = mage_chapters_list[-1]['name']
        #格式化的数据
        mage_name = self.changeChineseNumToArab(mage_name)
        mage_chapters_number = mage_chapters_list[-1]['_id']

        from_up_id = self.novel_chapter_res(form_path_chapters_list, mage_name)
        if from_up_id == 0:
            if mage_chapters_number > 12:
                lxsnumner = 11
            else:
                lxsnumner = mage_chapters_number

            #数据检查10次机会判断的信息的，如果的存在的就解析的成功
            for i in range(1,lxsnumner):
                mage_name = mage_chapters_list[-i]['name']
                #mage_chapters_number = mage_chapters_list[-i]['_id']
                mage_name = self.changeChineseNumToArab(mage_name)
                from_up_id = self.novel_chapter_res(form_path_chapters_list,mage_name)
                if from_up_id > 0:
                     break


        chapters_new_name = ''
        #可以进行小说合并的了
        if from_up_id:
            form_chapters_list = form_path_chapters_list[from_up_id::]
            if form_chapters_list:
                chapter_id = mage_chapters_number
                for index, vs in enumerate(form_chapters_list):

                    if from_up_id != vs['_id']:
                        form_valus_name = self.changeChineseNumToArab(vs['name'])
                        rec = any(map(FilterTag(form_valus_name.strip()), mage_chapters_list))
                        if not rec:
                            chapter_id += 1
                            source_list = []

                            new_chapters = {}
                            chapters_list_data = {}
                            chapters_list_data = {}
                            chapters_list_ads = {}
                            # 合并小说目录信息
                            new_chapters['_id'] = chapter_id
                            new_chapters['name'] = vs['name']
                            new_chapters['add_time'] = vs['add_time']

                            # 更新的新的章节
                            chapters_list_data['source_id'] = int(novel_chapters_info[update_novels_id])
                            chapters_list_data['url'] = vs['url']
                            chapters_list_data['add_time'] = vs['add_time']
                            chapters_list_data['weight'] = 1

                            source_list.append(chapters_list_data)
                            new_chapters['source_list'] = source_list
                            #new_chapter[str(chapter_id)] = new_chapters

                            #生成章节的目录列表
                            #self.add_novel_chapter_list(mage_novel_id, chapter_id, chapters_list_data)
                            mage_chapters_list.append(new_chapters)
                            chapters_new_name = vs.get("name")


                # print(mage_novel_id)
                # exit()
                if chapters_new_name:
                    if mage_chapters_list:
                        #添加的数据
                        self.insert_novel_chapter(mage_novel_id,mage_chapters_list)
                        # 更新章节的内容的问题
                        cursor = self.mysql.cursor()
                        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        count =  chapter_id
                        chapters_up_sql = 'update novel_chapters set is_sync=0,count=%s,new_name="%s",update_time="%s" where novels_id=%s' %(count,chapters_new_name,update_time,mage_novel_id)

                        cursor.execute(chapters_up_sql)

                        intosql = 'insert into update_chapters_list (chapters_name,update_time,novel_id,novel_name) VALUES'
                        intosql += '(%s,%s,%s,%s)'
                        cursor.execute(intosql, args=(chapters_new_name,update_time,mage_novel_id,mage_novel_name))
                        self.mysql.commit()
                        self.new_chapter_push(mage_novel_id,mage_novel_name)

                        msg = "合并成功小说ID：%s" %(mage_novel_id)
                        logging.info(msg)
            return mage_chapters_list
        else:

            file_draw_path = "/data/www/novel_script/logs/book_draw_%s.log" % (
            time.strftime("%Y-%m-%d", time.localtime()))
            with open(file_draw_path, "w") as acc_chapters_f:
                fidsd = "," + str(mage_novel_id)
                acc_chapters_f.write(fidsd)

            return  mage_chapters_list

    """
        获取的章节列表
    """

    def get_chapter_list(self, source_id, novels_id):

        if source_id == 35:
            novel_cel = novels_id % 10
            source_name = "source_%s" % (source_id)
            collection_name = "novel_%s" % (novel_cel)
            m_mongo = merge_mongo(source_name, collection_name)
            datas = m_mongo.m_chapters_list(novels_id)
        else:
            #数据集合的分区和分库
            source_cel = source_id % 10
            novels_cel = novels_id % 10
            source_name = "source_%s" % (source_cel)
            collection_name = "novel_%s" % (novels_cel)

            m_mongo = merge_mongo(source_name, collection_name)
            datas = m_mongo.m_chapters_list(novels_id)

        # new_datas = []
        # 格式化数据
        if datas:
            # for index in datas:
            #     new_datas.append(datas[index])
            #
            # datas = ''
            return datas
        else:
            return datas
    """
        添加的数据的信息
        字段说明
        novels_id 小说的ID
        chapter_data 格式化前的书的章节类别
    """
    def insert_novel_chapter(self,novels_id,chapter_data):
        novel_cel = novels_id % 10

        #TODO 章节的内容数据格式化
        new_chapter_data = {}

        # for vals in chapter_data:
        #     new_chapter_data[str(vals["_id"])] = vals

        novel_cel = novels_id % 10
        source_name = "source_%s" % (SOURCE_ID)
        collection_name = "novel_%s" % (novel_cel)
        m_mongo = merge_mongo(source_name, collection_name)
        status = m_mongo.m_chapters_count(novels_id)

        if status:
            updata = chapter_data
            new_chapter_data = ''
            if updata:
                rec = m_mongo.m_replace_chapters(novels_id, chapter_data)
                return rec
            else:
                return False
        else:
            rec = m_mongo.m_insert(chapter_data)
            new_chapter_data = ''
            chapter_data = ''
            return rec

    """
      修改章节下的来源信息
    """
    def update_novel_source_list(self,novels_id,chapters_id,source_list):
        novel_cel = novels_id % 10
        source_name = "source_%s" % (SOURCE_ID)
        collection_name = "novel_%s" % (novel_cel)
        m_mongo = merge_mongo(source_name, collection_name)
        m_mongo.m_update_source_list(novels_id,chapters_id,source_list)
        return True
    """
     新书的推送指定的Redis中在指定时间推送的到PHP MQ上
    """
    def new_chapter_push(self,novel_id,novel_name):
        recommend_redis_key = 'amqp:jpush'
        self.sredis.lpush(recommend_redis_key,json.dumps({'type': 2, 'id': novel_id, 'name': novel_name}, ensure_ascii=False))


    """
      查看的章节的最后匹配度
    """
    def novel_chapter_res(self,form_path_chapters_list,mage_name):
        from_up_id = 0
        for form_chapters_val in form_path_chapters_list[::-1]:
            rate = difflib.SequenceMatcher(None, mage_name, form_chapters_val['name']).quick_ratio()
            rate = int(round(rate, 2) * 100)
            # 就不用转换的了
            if rate > 90:
                from_up_id = form_chapters_val['_id']
                return  from_up_id
            elif rate > 50:
                form_chapters_name = self.changeChineseNumToArab(form_chapters_val['name'])
                rate = difflib.SequenceMatcher(None, mage_name, form_chapters_name).quick_ratio()
                rate = int(round(rate, 2) * 100)
                if rate > 90:
                    from_up_id = form_chapters_val['_id']
                    return  from_up_id

        return from_up_id
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
        大小写的转换的
    """
    def chinese2digits(self,uchars_chinese):
        total = 0
        r = 1  # 表示单位：个十百千...
        for i in range(len(uchars_chinese) - 1, -1, -1):
            val = common_used_numerals.get(uchars_chinese[i])
            if val >= 10 and i == 0:  # 应对 十三 十四 十*之类
                if val > r:
                    r = val
                    total = total + val
                else:
                    r = r * val
                    # total =total + r * x
            elif val >= 10:
                if val > r:
                    r = val
                else:
                    r = r * val
            else:
                total = total + r * val
        return total

    def changeChineseNumToArab(self,oriStr):
        lenStr = len(oriStr);
        aProStr = ''
        if lenStr == 0:
            return aProStr;

        hasNumStart = False;
        numberStr = ''
        for idx in range(lenStr):
            if oriStr[idx] in num_str_start_symbol:
                if not hasNumStart:
                    hasNumStart = True;

                numberStr += oriStr[idx]
            else:
                if hasNumStart:
                    if oriStr[idx] in more_num_str_symbol:
                        numberStr += oriStr[idx]
                        continue
                    else:
                        numResult = str(self.chinese2digits(numberStr))
                        numberStr = ''
                        hasNumStart = False;
                        aProStr += numResult

                aProStr += oriStr[idx]
                pass

        if len(numberStr) > 0:
            resultNum = self.chinese2digits(numberStr)
            aProStr += str(resultNum)

        return aProStr


# Redis 订阅接受者
ps = my_redis.pubsub()
ps.subscribe('UPNOVELS:CHAPTER')  #从UPNOVELS:CHAPTER订阅消息

for item in ps.listen():		#监听状态：有消息发布了就拿过来
    if item['type'] == 'message':
        novels_list = json.loads(item['data'])
        novel_up_model = novelMarge()
        msg = "更新记录小说：%s" % (item['data'])
        logging.info(msg)
        mage_novel_id = novels_list['new_novels_id']
        if mage_novel_id:
            novel_up_model.update_novel_gather(novels_list)

