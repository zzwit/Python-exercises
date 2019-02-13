import pymysql.cursors
import requests
import time
import redis
import json



# DB_HOST = '127.0.0.1'
# DB_USER = 'root'
# DB_PWD = 'root'
# DB_NAME = 'novel_collect'
# DB_CHARSET = 'utf8'
# DB_PORT = 3306
#
# REDIS_HOST = "127.0.0.1"
# REDIS_PORT = 6379
# REDIS_DB = 4
# REDIS_PWD = 'DH56ji2(3u^4'

DB_HOST = '127.0.0.1'
DB_USER = 'kusoucode'
DB_PWD = 'RvZ@7^yGR2waQNLJ'
DB_NAME = 'novel_collect'
DB_CHARSET = 'utf8'
DB_PORT = 3306

# #Redis
# REDIS_HOST = "61.160.196.39"
# REDIS_PORT = 26890
# REDIS_DB = 3
# REDIS_PWD = 'DH56ji2(3u^4'
#
#
# #Redis 链接
# my_redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PWD)


m_mysql = pymysql.Connect(
    host=DB_HOST,
    user=DB_USER,
    passwd=DB_PWD,
    port=DB_PORT,
    db=DB_NAME,
    charset=DB_CHARSET
)



"""
查询站点的可别
"""
def getSource():
    cursor = m_mysql.cursor()
    source_sql = "select verbose_name,`index`,id from  novel_source where is_spider=1 and is_delete = 0"
    cursor.execute(source_sql)
    data_list  = cursor.fetchall()
    for index,val in enumerate(data_list):
        requests.packages.urllib3.disable_warnings()
        html = requests.get(val[1], verify=False)

        respon = html.status_code
        print(respon)
        if(respon!=200):
            #发生报警
            sendSms(val[2])
            time.sleep(200)
def sendSms(id):

    #content =
    id = str(id)
    content= "【酷搜APP】" +"提示：("+id+")请查看"
    #content = content.encode("utf-8")
    account = 800957
    password = 'MKyn81bN'
    mobile = '18612598083,18630099352,18401323238,18738993121,18222670756'
    url = "http://39.106.108.70:7803/sms?action=send&account=%s&password=%s&mobile=%s&content=%s&extno=106902202230&rt=json" %(account,password,mobile,content)
    html = requests.get(url)


if __name__=='__main__':
    getSource()
