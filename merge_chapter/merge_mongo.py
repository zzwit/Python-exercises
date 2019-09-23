from pymongo import MongoClient
import json
"""

{
    "_id": 4,
    "chapter_list":
    {
        "1": {
            "name":"第1章 飞来横祸",
            "add_time": "2019-01-14 22:45:19",
            "source_list" : [
                {
                    "source_id" : 1,
                    "url" : "/book/35034/21389240.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                },
                {
                    "source_id" : 2,
                    "url" : "/book/35034/21389226.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                }
            ]
        },

        "2": {
            "name":"第2章 飞来横祸",
            "add_time": "2019-01-14 22:45:19",
            "source_list" : [
                {
                    "source_id" : 1,
                    "url" : "/book/35034/21389240.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                },
                {
                    "source_id" : 2,
                    "url" : "/book/35034/21389226.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                }
            ]
        }
    }

}



"""

novels = {
    "_id": 4,
    "chapters":
    {
        "1": {
            "_id":1,
            "name":"第1章 飞来横祸",
            "add_time": "2019-01-14 22:45:19",
            "url":"",
            "source_list" : [
                {
                    "source_id" : 1,
                    "url" : "/book/35034/21389240.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                },
                {
                    "source_id" : 2,
                    "url" : "/book/35034/21389226.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                },


            ]
        },

        "2": {
            "_id":2,
            "name":"第2章 飞来横祸",
            "add_time": "2019-01-14 22:45:19",
            "source_list" : [
                {
                    "source_id" : 1,
                    "url" : "/book/35034/21389240.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                },
                {
                    "source_id" : 2,
                    "url" : "/book/35034/21389226.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                }
            ]
        },
        "3": {
            "_id":3,
            "name":"第3章 飞来横祸",
            "add_time": "2019-01-14 22:45:19",
            "source_list" : [
                {
                    "source_id" : 1,
                    "url" : "/book/35034/21389240.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                },
                {
                    "source_id" : 2,
                    "url" : "/book/35034/21389226.html",
                    "add_time": "2019-01-14 22:45:19",
                    'weight':2,
                }
            ]
        }
    }

}



MG_HOST = "192.168.0.39"
MG_PORT = 27018
MG_USER = "admin"
MG_PWD  = "admin@1234"

FILE_BASE = "/data/book/"

class merge_mongo(object):
    def __init__(self,db_name,collection_name):
        client = MongoClient(MG_HOST, MG_PORT, username=MG_USER, password=MG_PWD)
        self.dbcle = client[db_name]
        self.collection = self.dbcle[collection_name]

    """
        添加整个大的Data
    """
    def m_insert(self,data):
        rec = self.collection.insert_one(data)
        return rec.inserted_id

    """
       查询的整个
    """
    def m_select_find_one(self,id):
        return self.collection.find_one({"_id":id})


    """
    
    查询的 指定的要显示、chapters列表和 source_list 集合信息
    字段的说明： novel_id 小说ID  chapters_id 章节ID
    查看指定字段 data['chapters']['1']["source_list"] 这样的方式
    """
    def m_source_list(self,novel_id,chapters_id):
        #条件的
        where = {
            "_id":novel_id
        }
        chapters = "chapters.%s" %(chapters_id)
        #展示的字段
        show_field = {
            chapters:1
        }

        return  self.collection.find_one(where,show_field)

    """
        判断的是否在里面的信息
        如果存在 返回 数量的，如果不存在返回0
    """
    def m_source_id_count(self,novel_id,chapters_id,source_id):

        wheres = "chapters.%s.source_list.source_id" %(chapters_id)
        # 查询条件
        where = {
            "_id":novel_id,
            wheres:source_id
        }

        return self.collection.find(where).count()


    """
        获取的小说的章节的列表信息
    """
    def m_chapters_list(self,novel_id):
        where = {
            "_id":novel_id
        }

        show_field = {
            "chapters": 1
        }

        data = self.collection.find_one(where, show_field)
        if data:
            return data.get("chapters")
        else:
            return None

    """
        返回的 0 是当前的 有小说的信息但是没有的 章节  大于 0 是有章节  None 是没有当期小说信息
    """
    def m_chapters_count(self,novel_id):
        where = {
            "_id":novel_id
        }

        show_field = {
            "chapters": 1
        }

        data = self.collection.find_one(where, show_field)
        if data:
            return  len(data["chapters"])
        else:
            return  None
    """
      其实的是修改的小说的信息 只是添加的章节的信息一个
      添加的章节列表信息
      db.getCollection('novel_21').update({"_id":4},{"$set":{"chapters.3":{"_id":"3","name" : "第3章 飞来横祸","add_time" : "2019-01-14 22:45:19","source_list" : [{"source_id" : "1","url" : "/book/35034/21389240.html","add_time" : "2019-01-14 22:45:19","weight" : "2"
                }, {"source_id" : "2","url" : "/book/35034/21389226.html","add_time" : "2019-01-14 22:45:19","weight" : "2"}]}}})
      
      
    添加的单个的章节的更新信息
    updates = {
            "4": {
                "_id":"4",
                "name":"第4章 飞来横祸",
                "add_time": "2019-01-14 22:45:19",
                "source_list" : [
                    {
                        "source_id" : "1",
                        "url" : "/book/35034/21389240.html",
                        "add_time": "2019-01-14 22:45:19",
                        'weight':"2",
                    },
                    {
                        "source_id" : "2",
                        "url" : "/book/35034/21389226.html",
                        "add_time": "2019-01-14 22:45:19",
                        'weight':"1",
                    }
                ]
            }
    }
    """
    def m_update_chapters(self,novel_id,chapters_id,chapters_data):
        chapters_s = "chapters.%s" %(chapters_id)
        self.collection.update({"_id":novel_id},{"$set":{chapters_s:chapters_data}})

    """
    
        修改的章节的来源的列表
    """
    def m_update_source_list(self,novel_id,chapters_id,source_list):
        chapters_id = int(chapters_id)
        chapters_s = "chapters.$.source_list"
        rec = self.collection.update({"_id": novel_id,"chapters._id": chapters_id}, {"$set": {chapters_s: source_list}})
        return rec['n']

    """
         替换的当前的小说的所有的章节信息
    """
    def m_replace_chapters(self,novel_id,chapters_data):
        rec = self.collection.update({"_id": novel_id}, {"$set": {"chapters": chapters_data}})
        return rec['n']
    """
    更新记录
    """

# def get_file_data():
#     fromPath = "%s%s/%s/chapter.json" % (FILE_BASE, 1, 883)
#
#     with open(fromPath, 'r') as f:
#         flist = f.read()
#         if flist:
#             chapter_list = json.loads(flist)
#
#         else:
#             chapter_list = ""
#             print("目录没有内容")
#
#     # new_chapter_list = {}
#     # print(chapter_list)
#     # exit()
#     # for index,val in enumerate(chapter_list):
#     #     index += 1
#     #     stindex = str(index)
#     #     new_chapter_list[stindex] = val
#
#     novels_data = {
#         "_id":165465,
#         "chapters":chapter_list
#     }
#
#     source_id = 2%10
#     source_35 = "source_%s" %(source_id)
#     novel_ids = 165465%10
#     collection_name = "novel_%s" %(novel_ids)
#     # print(source_35)
#     # print(collection_name)
#     # exit()
#     t = merge_mongo(source_35, collection_name)
#     t.m_insert(novels_data)
# get_file_data()




#
# source_35 = "source_35"
# collection_name = "novel_21"
# t = merge_mongo(source_35,collection_name)
# data = t.m_replace_chapters(4,novels)
# print(data)
# exit()
