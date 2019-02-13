import sys
import pika

"""
工作队列（任务队列）

在第一篇教程中，我们已经写了一个从已知队列中发送和获取消息的程序。在这篇教程中，我们将创建一个工作队列（Work Queue），它会发送一些耗时的任务给多个工作者（Worker）。

工作队列（又称：任务队列——Task Queues）是为了避免等待一些占用大量资源、时间的操作。当我们把任务（Task）当作消息发送到队列中，一个运行在后台的工作者（worker）进程就会取出任务然后处理。当你运行多个工作者（workers），任务就会在它们之间共享。

这个概念在网络应用中是非常有用的，它可以在短暂的HTTP请求中处理一些复杂的任务。

"""

# 链接的服务
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
#生产的句柄
channel = connection.channel()

channel.queue_declare(queue="hello")

message = ' '.join(sys.argv[1:]) or "Hello World!"

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=message)

print (" [x] Sent ",message)