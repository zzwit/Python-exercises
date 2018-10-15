#!/usr/local/bin/python3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pymysql.cursors
import datetime
import math
import os



#数据连接地址
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
clickDataFilePath = "/clicks/"  #点击数据目录
scatterFilePath  = "/clickscatter/" # 散点图地址
warningFilePath  = "/clickwarning/" #异常图片地址

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
    #sql = "SELECT siteid,SUM(sumpay) AS sumpay,adstypeid FROM dr_stats_lastmonth WHERE DAY='%s' AND adstypeid in(2,4) GROUP BY siteid,adstypeid " %('2017-05-01')
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    # data = ((13861, 18669.5666, 2), (13861, 100.0000, 4))
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

    cickwarningPath1 = "%s%s%s.png" % (fileBase, warningFilePath, clickName1)  # 异常图片地址
    cickwarningPath2 = "%s%s%s.png" % (fileBase, warningFilePath, clickName2)  # 异常图片地址

    if os.path.exists(fileName1Path):  # 如果文件存在
        os.remove(fileName1Path)  # 则删除

    if os.path.exists(fileName2Path):  # 如果文件存在
        os.remove(fileName2Path)  # 则删除

    if os.path.exists(cickscatterPath1):  # 如果文件存在
        os.remove(cickscatterPath1)  # 则删除

    if os.path.exists(cickscatterPath2):  # 如果文件存在
        os.remove(cickscatterPath2)  # 则删除

    if os.path.exists(cickwarningPath1):  # 如果文件存在
        os.remove(cickwarningPath1)  # 则删除

    if os.path.exists(cickwarningPath2):  # 如果文件存在
        os.remove(cickwarningPath2)  # 则删除


#执行的程序
# siteId 站长ID
# type 类型 2横幅,4贴片
def runck(siteId,type):
    fileName = "%s_%s_%s" % (siteId, type, yesterdayv)
    # 文件数据存储的地址
    cickDataPath = "%s%s%s.txt" %(fileBase,clickDataFilePath,fileName) # 点击数据目录

    cickscatterPath = "%s%s%s.png" %(fileBase,scatterFilePath,fileName) # 散点图地址
    cickwarningPath  =  "%s%s%s.png" %(fileBase,warningFilePath,fileName) # 异常图片地址

    if  os.path.exists(cickDataPath) != True :
        return 2

    reader = pd.read_csv(cickDataPath, iterator=True)

    # 限制最多获取的数据资源
    chunkSize = 200000
    chunks = []
    try:

        chunk = reader.get_chunk(chunkSize)
        height = chunk['cy']
        weight = chunk['cx']
        plt.axis('on')  # 关掉坐标轴为 off
        plt.scatter(weight, height)
        plt.xlim(0, 370)
        plt.ylim(0, 650)

        # 设置title和x，y轴的label
        titlename = "SiteID(%s) and Type(%s) %s Click distribution graph" %(siteId,type,yesterdayv)
        plt.title(titlename)
        plt.xlabel("X")
        plt.ylabel("Y")

        plt.savefig(cickscatterPath)  # 保存图片
        plt.close()
        # 分块初始计算 1 头部 2 左下边 3 右下边
        count_dict = {1: 0, 2: 0, 3: 0}
        # 获取的几个模块
        zdata = getBlockCp()
        # 检查坐标是在面积之内
        for cke, cx in enumerate(chunk['cx']):
            for key, val in enumerate(zdata):
                keyval = key + 1
                cxnmv = is_number(cx)
                cynmv = is_number(chunk['cy'][cke])
                if  cxnmv == True &  cynmv == True :
                    status = isInside(val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7], cx, chunk['cy'][cke])
                    if status == True:
                        count_dict[keyval] += 1
                        break
                else:
                    break
        # 数据总数
        cnumber = cke + 1
        # 横幅的模块
        titleBname = "SiteID(%s) and Type(%s) %s TotalNumber %s" % (siteId, type, yesterdayv,cnumber)
        fig, ax = plt.subplots(figsize=(7, 5), subplot_kw=dict(aspect="equal"))
        # 14616
        #labels = [u'headBlock\n', u'LeftBlock\n', u'RightBlock\n', u'OtherBlock\n']

        ingredients = ['headBlock','LeftBlock','RightBlock','OtherBlock']
        data = [count_dict[1],count_dict[2],count_dict[3],(cnumber - count_dict[1] - count_dict[2] - count_dict[3])]
        # fracs = [count_dict[1] / cnumber * 100, count_dict[2] / cnumber * 100, count_dict[3] / cnumber * 100,
        #          (cnumber - count_dict[1] - count_dict[2] - count_dict[3]) / cnumber * 100]


        wedges, texts, autotexts = ax.pie(data, autopct=lambda pct: func(pct, cnumber),
                                          textprops=dict(color="w"))

        # explode = [x * 0.01 for x in range(1,5)]  # 与labels一一对应，数值越大离中心区越远
        # plt.axes(aspect=1)  # set this , Figure is round, otherwise it is an ellipse
        # # autopct ，show percet
        # plt.pie(x=fracs, labels=labels, explode=explode, autopct='%3.1f %%',
        #         shadow=True, labeldistance=1.1, startangle=90, pctdistance=0.6
        #
        #         )
        #plt.legend(loc=7, bbox_to_anchor=(1.2, 0.80), ncol=3, fancybox=True, shadow=True, fontsize=8)
        ax.legend(wedges, ingredients,
                  title="Ingredients",
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1))

        plt.setp(autotexts, size=8, weight="bold")

        ax.set_title(titleBname)

        if type ==2 & cnumber > 20 :
            if count_dict[1] < 1 :
                plt.savefig(cickwarningPath)  # 保存图片
                plt.close()
                return 0
        if type == 4 & cnumber > 20:
            if count_dict[3] < 1 :
                plt.savefig(cickwarningPath)  # 保存图片
                plt.close()
                return 0
        plt.close()
        return  1
    except StopIteration:
        print("Iteration is stopped.");

def func(pct, allvals):
    absolute = int(pct / 100. * allvals)
    return "{:.1f}%".format(pct)


# 将手机尺寸分成不同 区域
def getBlockCp():
    Block = [
        [0, 0, 0, 200, 800, 0, 800, 200],
        [0, 200, 0, 1000, 150, 200, 150, 1000],
        [150, 200, 150, 1000, 800, 200, 800, 1000]
    ]
    return Block


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


# 判断的这个点是否在矩形面积内 (x1,y1)为最左的点,(x2,y2)为左最上的点,(x3, y3)为右最下的点,(x4, y4)为最右的点。给定4个点代表的矩形，再给定一个点(x, y)，判断(x, y)是否在矩形中。
def isInside(x1, y1, x2, y2, x3, y3, x4, y4, x, y):
    x = float(x)
    y = float(y)
    def isInside(x1, y1, x4, y4, x, y):
        if x <= x1 or x >= x4 or y <= y4 or y >= y1:
            return False
        return True

    if y1 == y2:
        return isInside(x1, y1, x4, y4, x, y)
    a = abs(y4 - y3)
    b = abs(x4 - x3)
    c = math.sqrt(a * a + b * b)
    sin = a / c
    cos = b / c
    x1R = x1 * cos + y1 * sin
    y1R = y1 * cos - x1 * sin
    x4R = x4 * cos + y4 * sin
    y4R = y4 * cos - x4 * sin
    xR = x * cos + y * sin
    yR = y * cos - x * sin
    return isInside(x1R, y1R, x4R, y4R, xR, yR)


if __name__ == "__main__":
    main()
