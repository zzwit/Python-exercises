from pymongo import MongoClient

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
host = '127.0.0.1'
client = MongoClient(host, 27017,username="admin",password="admin@1234")
#连接mydb数据库,账号密码认证
db = client.source_35    # mydb数据库，同上解释
collection = db.novel_1   # myset集合，同上解释
collection.insert(novels)   # 插入一条数据，如果没出错那么说明连接成功