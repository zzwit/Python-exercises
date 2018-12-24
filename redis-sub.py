import redis
import time

"""
Redis 实现的订阅  点阅者

"""

rc = redis.StrictRedis(host='127.0.0.1', port='6379', db=3, password='')


ps = rc.pubsub()
ps.subscribe('liao')  #从liao订阅消息
for item in ps.listen():		#监听状态：有消息发布了就拿过来
    if item['type'] == 'message':
        time.sleep(3)
        print(item['channel'])
        print(item['data'])
