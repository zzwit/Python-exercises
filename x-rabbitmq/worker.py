import pika
import time
"""
 工作队列
"""
# 实例化 Mq
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
#生产的句柄
channel = connection.channel()

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count('.'))
    print( " [x] Done")


channel.basic_consume(callback,queue="hello",no_ack=True)


print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
