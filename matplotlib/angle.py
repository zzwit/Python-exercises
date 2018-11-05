#!/usr/local/bin/python3
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import pymysql.cursors
import datetime
import math
import os
import time
from numpy import *
# #数据连接地址

dbhost = '127.0.0.1'
dbuser = 'root'
dbpwd = 'root'
dbname = 'dianr2'
dbcharset = 'utf8'
dbport = 3306

today = datetime.date.today()
yester = today - datetime.timedelta(days=1)
yesterday = yester.strftime('%Y-%m-%d')  # 格式化时间 获取的昨天的时间
yesterdayv = yester.strftime('%Y%m%d')  # 格式化时间 获取的昨天的时间

# 数据保存2天
daty2 = today - datetime.timedelta(days=2)
daty2 = daty2.strftime('%Y%m%d')  # 格式化前3天 时间

# 图片保存时间30天
daty30 = today - datetime.timedelta(days=30)
daty30 = daty30.strftime('%Y%m%d')  # 格式化前30天 时间

fileBase = "/data/www/mm/tmp" # 文件根目录
clickDataFilePath = "/angle/"  #点击数据目录
scatterFilePath  = "/angleimg/" # 散点图地址
# 设置字体
myfont = matplotlib.font_manager.FontProperties(
    fname='/usr/local/lib/python3.6/site-packages/matplotlib/mpl-data/fonts/ttf/simhei.ttf')



# myfont = matplotlib.font_manager.FontProperties(
#     fname='/Library/Frameworks/Python.framework/Versions/3.6/lib/python3.6/site-packages/matplotlib/mpl-data/fonts/ttf/simhei.ttf')

def main():
    start = datetime.datetime.now()
    db = pymysql.Connect(
        host=dbhost,
        port=dbport,
        user=dbuser,
        passwd=dbpwd,
        db=dbname,
        charset=dbcharset
    )
    cursor = db.cursor()
    sql = "SELECT siteid,SUM(sumpay) AS sumpay,adstypeid FROM dr_stats_loc WHERE DAY='%s' AND adstypeid in(2,4) GROUP BY siteid,adstypeid having SUM(sumpay) > 50 order by  SUM(sumpay) desc  " %(yesterday)
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    # data = ( (70, 100.0000, 4),(70, 18669.5666, 2))
    i = 0;
    tonumber = 0
    for stids in data:
        tonumber +=1
        #deleteClickFile(stids[0]) #执行删除前3天的数据
        # if stids[1] > 50:
        rec = runck(stids[0], stids[2])
        if rec ==0 :
            i += 1
            print("日期 %s 异常站长ID %d 类型%s" % (yesterday, stids[0],stids[2]))
            #写入日志文件
        print("日期 %s 数据更新站长ID %d " %(yesterday,stids[0]))
    end = datetime.datetime.now()
    print('日期 %s 异常总数量 %d' %(yesterday,i))
    print('日期 %s 总共更新 %d' %(yesterday,tonumber))
    print('日期 %s 总共执行时间 %s' %(yesterday,end-start))

# 删除前2天的点击的数据
def  deleteClickFile(siteId):

    fileName1 = "%s_%s_%s" % (siteId, 2, daty2)
    fileName2 = "%s_%s_%s" % (siteId, 4, daty2)

    fileName1Path = "%s%s%s.txt" % (fileBase,clickDataFilePath,fileName1)  # 点击数据目录
    fileName2Path = "%s%s%s.txt" % (fileBase,clickDataFilePath,fileName2)  # 点击数据目录
    clickName1 = "%s_%s_%s" % (siteId, 2, daty30)
    clickName2 = "%s_%s_%s" % (siteId, 4, daty30)

    cickscatterPath1 = "%s%s%s.png" % (fileBase, scatterFilePath, clickName1)  # 散点图地址
    cickscatterPath2 = "%s%s%s.png" % (fileBase, scatterFilePath, clickName2)  # 散点图地址


    if os.path.exists(fileName1Path):  # 如果文件存在
        os.remove(fileName1Path)  # 则删除

    if os.path.exists(fileName2Path):  # 如果文件存在
        os.remove(fileName2Path)  # 则删除

    if os.path.exists(cickscatterPath1):  # 如果文件存在
        os.remove(cickscatterPath1)  # 则删除

    if os.path.exists(cickscatterPath2):  # 如果文件存在
        os.remove(cickscatterPath2)  # 则删除


#执行的程序
# siteId 站长ID
# type 类型 2横幅,4贴片
def runck(siteId,type):

    fileName = "%s_%s_%s" % (siteId, type, yesterdayv)
    # 文件数据存储的地址
    cickDataPath = "%s%s%s.txt" %(fileBase,clickDataFilePath,fileName) # 点击数据目录

    cickscatterPath = "%s%s%s.png" %(fileBase,scatterFilePath,fileName) # 散点图地址

    if  os.path.exists(cickDataPath) != True :
        return 2

    reader = pd.read_csv(cickDataPath, iterator=True)
    # 限制最多获取的数据资源
    chunkSize = 200000
    chunks = []
    try:

        chunk = reader.get_chunk(chunkSize)
        #白天 8点多到晚上12点   6点到10点上班时间  10点到12点  中午12点到2点  2点到 5：30点 下班时间 5：30 到10 点  睡觉 11点到 5点
        dateData = chunk['datetime']
        jdData   = chunk['jd']

        sumdata1 = array([1, 1, 1, 1, 1, 1]) #睡觉 10点到 6点
        sumdata2 = array([1, 1, 1, 1, 1, 1]) #6点到10点上班时间 活跃
        sumdata3 = array([1, 1, 1, 1, 1, 1]) #10点到12点
        sumdata4 = array([1, 1, 1, 1, 1, 1]) #中午12点到2点    活跃
        sumdata5 = array([1, 1, 1, 1, 1, 1]) #2点到 5：30点
        sumdata6 = array([1, 1, 1, 1, 1, 1]) #下班时间 5：30 到10 点   活跃
        # sumdata7 = array([1, 1, 1, 1, 1, 1]) # 其他时间

        ctData = [[0,20],[20,40],[40,85],[90,110],[110,160],[160,180]]
        #晚上  110,160 这个活跃度比较高，晚上躺着看手机读小说   时间点统计 0 - 8点
        snumabc = 0
        sumdata1Sum = 6
        sumdata2Sum = 6
        sumdata3Sum = 6
        sumdata4Sum = 6
        sumdata5Sum = 6
        sumdata6Sum = 6
        # sumdata7Sum = 6
        # 整理数据格式化
        for ck,jds in enumerate(jdData):
            snumabc+=1
            h, m, s = dateData[ck].strip().split(":")
            h = int(h)
            for key,iv in enumerate(ctData):
                 if jds>iv[0] and jds<iv[1]:
                     if h > 6 and h < 10:
                         sumdata2Sum +=1
                         sumdata2[key]+=1
                         break
                     elif h>10 and h<12:
                         sumdata3Sum +=1
                         sumdata3[key] += 1
                         break
                     elif h>12 and h<14:
                         sumdata4Sum +=1
                         sumdata4[key] += 1
                         break
                     elif h > 14 and h < 18:
                         sumdata5Sum += 1
                         sumdata5[key] += 1
                         break
                     elif h > 18 and h < 20:
                         sumdata6Sum += 1
                         sumdata6[key] += 1
                         break
                     elif h > 20 and h < 6:
                         sumdata1Sum += 1
                         sumdata1[key] += 1
                         break

        sumdata1 = list(sumdata1)
        sumdata2 = list(sumdata2)
        sumdata3 = list(sumdata3)
        sumdata4 = list(sumdata4)
        sumdata5 = list(sumdata5)
        sumdata6 = list(sumdata6)

        fig, axs = plt.subplots(nrows=2, ncols=3,figsize=(11, 7),constrained_layout=True)

        labels = '0°-20°','20°-40°','40°-85°','90°-110°','110°-160°','160°-180°'
        axs[0, 0].set_title('20点到06点参考数(%s)统计'%(sumdata1Sum), fontproperties=myfont)
        axs[0, 0].pie(sumdata1, labels=labels, autopct='%1.1f%%', shadow=True)

        # axs[0, 1].pie(sumdata2, labels=labels, autopct='%.0f%%', shadow=True,
        #               =(0, 0.1explode, 0, 0))
        axs[0, 1].set_title('06点到10点参考数(%s)统计'%(sumdata2Sum),fontproperties=myfont)
        axs[0, 1].pie(sumdata2, labels=labels, autopct='%.0f%%', shadow=True)

        axs[0, 2].set_title('10点到12点参考数(%s)统计'%(sumdata3Sum),fontproperties=myfont)
        axs[0, 2].pie(sumdata3, labels=labels, autopct='%.0f%%', shadow=True)

        axs[1, 0].set_title('12点到14点参考数(%s)统计'%(sumdata4Sum),fontproperties=myfont)
        patches, texts, autotexts = axs[1, 0].pie(sumdata4, labels=labels,
                                                  autopct='%.0f%%',
                                                  textprops={'size': 'smaller'},
                                                  shadow=True)
        axs[1, 1].set_title('16点到18点参考数(%s)统计'%(sumdata5Sum),fontproperties=myfont)
        patches, texts, autotexts = axs[1, 1].pie(sumdata5, labels=labels,
                                                  autopct='%.0f%%',
                                                  textprops={'size': 'smaller'},
                                                  shadow=True)
        axs[1, 2].set_title('18点到20点参考数(%s)统计'%(sumdata6Sum),fontproperties=myfont)
        patches, texts, autotexts = axs[1, 2].pie(sumdata6, labels=labels,
                                                  autopct='%.0f%%',
                                                  textprops={'size': 'smaller'},
                                                  shadow=True)
        if(type ==2):
            typetilte = "横幅(%s)" %(type)
        elif (type == 4):
            typetilte = "贴片(%s)" % (type)
        else:
            typetilte = "其他"

        titleBname = "日期：%s 站点ID(%s) %s 参考数(%s)手机角度统计" %(yesterday,siteId,typetilte,snumabc)
        plt.suptitle(titleBname, color='r', fontproperties=myfont,fontsize='large')

        # plt.legend(('0-20', '20-40', '40-85', '90-110', '110-160', '160-180'),
        #            loc='upper left')

        plt.savefig(cickscatterPath)  # 保存图片
        # plt.show()
        plt.close()

    except StopIteration:
        print("Iteration is stopped.");

if __name__ == "__main__":
    main()
