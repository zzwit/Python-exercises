import redis
import time
import pymysql.cursors
import difflib
import logging
import json
import os

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
#
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

SOURCE_ID = 555333


#过滤器
class FilterTag(object):

    def __init__(self, separator=u' '):
        self.separator = separator

    def __call__(self, values):

        valus_name = values['name'].strip()
        novelMargeModel = novelMarge()
        valus_name = novelMargeModel.changeChineseNumToArab(valus_name)
        filter_rate = difflib.SequenceMatcher(None, self.separator,valus_name).quick_ratio()
        filter_rate = int(round(filter_rate, 2) * 100)
        if filter_rate < 90:
            return 0
        else:
            return 1

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

            str = novels_list['name_author']
            mage_novel_name = str.split('$c')[0]


            # 获取的所有的相关的章节的最近更新的数量
            novel_chapters_sql = "select novels_id,count from  novel_chapters where novels_id IN(%s) order by count desc " % (up_novel_id)
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
            novel_novels_neaten_sql = "select novels_id,source_id,count from  novel_novels_neaten where novels_id IN(%s)" % (up_novel_id)
            cursor.execute(novel_novels_neaten_sql)
            novel_novels_neaten = cursor.fetchall()


            # 获取的合并的书籍所有的章节内容
            newsPath = "%s%s/%s/chapter.json" % (FILE_BASE, SOURCE_ID, mage_novel_id)
            new_path_recss = os.path.exists(newsPath)
            if new_path_recss == False:
                file_error_path = "/data/www/novel_script/logs/book_non_existent_%s.log" %(time.strftime("%Y-%m-%d", time.localtime()))
                with open(file_error_path, "a+") as file_error:
                    error_newsPath = newsPath+'\n'
                    file_error.write(error_newsPath)
            else:
                with open(newsPath, 'r') as f:
                    mage_chapters_list = f.read()
                    mage_chapters_list = json.loads(mage_chapters_list)

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


                                        # 合并之后的数据
                                        mage_chapters_list_file_path = "%s%s/%s/%d/list.json" % (FILE_BASE, SOURCE_ID, mage_novel_id, mage_vals['_id'])

                                        file_status = os.access(mage_chapters_list_file_path, os.R_OK | os.W_OK | os.X_OK)
                                        if file_status == False:
                                            # file_error_path = "/data/www/novel_script/logs/book_power_%s.log" %(time.strftime("%Y-%m-%d", time.localtime()))
                                            # with open(file_error_path, "a+") as file_error:
                                            #     error_book_id = ','+str(mage_novel_id)
                                            #     file_error.write(error_book_id)
                                            continue
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

                                            msg = "原站点%s更新至35 原书籍ID:%s更新的至书籍ID%s更新章节：%s" %(form_source_id,neaten_value[0],mage_novel_id,mage_vals['_id'])
                                            logging.info(msg)
                                            # 更新的当前的书籍更新的总数

                                        else:
                                            logging.info(form_name_msg+"章节的已经更新到最新")

                            # update_novel_novels_neaten_sql = "update novel_novels_neaten set count=%s where novels_id=%s" % (form_chapters_count, neaten_value[0])
                            # cursor.execute(update_novel_novels_neaten_sql)
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


        form_path_list = "%s%s/%s/chapter.json" % (FILE_BASE, novel_chapters_info[update_novels_id], update_novels_id)

        form_recs = os.path.exists(form_path_list)

        # 判断的文件是否存在
        if form_recs == False:

            msg = "书籍ID%s没有内容" % (update_novels_id)
            logging.info(msg)
            return mage_chapters_list


        with open(form_path_list, 'r') as form_path_f:
            form_path_chapters_list = form_path_f.read()
            if form_path_chapters_list:
                form_path_chapters_list = json.loads(form_path_chapters_list)
            else:
                return mage_chapters_list



        mage_name = mage_chapters_list[-1]['name']
        #格式化的数据
        mage_name = self.changeChineseNumToArab(mage_name)
        mage_chapters_number   = mage_chapters_list[-1]['_id']
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
                            chapters_list_data = {}
                            chapters_list_ads = {}
                            # 更新的新的章节
                            chapters_list_data['source_id'] = novel_chapters_info[update_novels_id]
                            chapters_list_data['url'] = vs['url']
                            chapters_list_data['add_time'] = vs['add_time']
                            chapters_list_data['weight'] = 1
                            #合并小说目录信息
                            chapters_list_ads['_id'] = chapter_id
                            chapters_list_ads['name'] = vs['name']
                            chapters_list_ads['add_time'] = vs['add_time']
                            #生成章节的目录列表
                            self.add_novel_chapter_list(mage_novel_id, chapter_id, chapters_list_data)
                            mage_chapters_list.append(chapters_list_ads)
                            chapters_new_name = vs['name']



                if chapters_new_name:
                    if mage_chapters_list:
                        with open(newsPath, 'w') as f:
                            f.write(json.dumps(mage_chapters_list, ensure_ascii=False))

                    # 更新章节的内容的问题
                    # cursor = self.mysql.cursor()
                    # update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # count =  chapter_id
                    # chapters_up_sql = 'update novel_chapters set is_sync=0,count=%s,new_name="%s",update_time="%s" where novels_id=%s' %(count,chapters_new_name,update_time,mage_novel_id)
                    #
                    # cursor.execute(chapters_up_sql)
                    #
                    # instr = 'insert into update_chapters_list (chapters_name,update_time,novel_id,novel_name) VALUES'
                    # instr += '(%s,%s,%s,%s)'
                    # cursor.execute(instr, args=(chapters_new_name,update_time,mage_novel_id,mage_novel_name))
                    # self.mysql.commit()
                    #self.new_chapter_push(mage_novel_id,mage_novel_name)

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

    def add_novel_chapter_list(self, novels_id, chapter_id, data):

        path = "%s%s/%s/" % (FILE_BASE, SOURCE_ID, novels_id)
        file_path = path + "list_" + str(chapter_id) + ".json"
        rec = os.path.exists(file_path)
        list_data = []
        if rec == True:
            with open(file_path, 'r') as fr:
                lists = fr.read()
                if lists:
                    list_data = json.loads(lists)
                else:
                    list_data = []
        with open(file_path, mode='w') as f:
            if rec == True:
                source_id_rel = 0
                for ival in list_data:
                    if data['source_id'] == ival['source_id']:
                        source_id_rel = 1
                if source_id_rel == 0:
                    list_data.append(data)
            else:
                list_data.append(data)
            f.write(json.dumps(list_data, ensure_ascii=False))
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
        msg = "更新小说：%s" % (item['data'])
        logging.info(msg)
        mage_novel_id = novels_list['new_novels_id']
        if mage_novel_id:
            time.sleep(4)
            novel_up_model.update_novel_gather(novels_list)
            print("成功")

