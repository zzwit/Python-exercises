import redis
import time

"""
redis 订阅者

"""

number_list = ['300035','300034','300033', '300032', '300031', '300030']

signal = ['1', '-1','1', '-1', '1', '-1']


rc = redis.StrictRedis(host='127.0.0.1', port='6379', db=3, password='')


for i in range(len(number_list)):
    value_new = str(number_list[i]) + ' ' + str(signal[i])
    rc.publish("liao", value_new)  #发
    print("发送",value_new)
