import sys
import pika

"""
消息持久化

如果你没有特意告诉RabbitMQ，那么在它退出或者崩溃的时候，将会丢失所有队列和消息。为了确保信息不会丢失，有两个事情是需要注意的：我们必须把“队列”和“消息”设为持久化。
首先，为了不让队列消失，需要把队列声明为持久化（durable）：

"""

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Hello World!"

# 这时候，我们就可以确保在RabbitMq重启之后queue_declare队列不会丢失。另外，我们需要把我们的消息也要设为持久化——将delivery_mode的属性设为2。

channel.basic_publish(exchange='',
                      routing_key='task_queue',
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))

"""
注意：消息持久化
    将消息设为持久化并不能完全保证不会丢失。以上代码只是告诉了RabbitMq要把消息存到硬盘，但从RabbitMq收到消息到保存之间还是有一个很小的间隔时间。因为RabbitMq并不是所有的消息都使用fsync(2)——它有可能只是保存到缓存中，并不一定会写到硬盘中。并不能保证真正的持久化，但已经足够应付我们的简单工作队列。如果你一定要保证持久化，你需要改写你的代码来支持事务（transaction）。
"""

print (" [x] Sent ",message)

connection.close()