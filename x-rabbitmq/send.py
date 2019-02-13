import pika
import json
"""
 RabbitMq 库学习（http://rabbitmq.mr-ping.com/tutorials_with_python/[1]Hello_World.html）
 入门学习 ， 编写一个 Hello World!
 不同的编程语言拓展的链接地址（https://www.rabbitmq.com/devtools.html）
 Python 推荐推展 （Pika）
"""



path = "./xiaowen.txt"
with open(path,'r') as f:
    flist = f.read()
    if flist:
        print(json.loads(flist))


exit()
# 链接的服务
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
#生产的句柄
channel = connection.channel()
"""
在发送消息之前，我们需要确认服务于消费者的队列已经存在。如果将消息发送给一个不存在的队列，RabbitMQ会将消息丢弃掉。下面我们创建一个名为"hello"的队列用来将消息投递进去。
"""
channel.queue_declare(queue="hello")

"""
在RabbitMQ中，消息是不能直接发送到队列中的，这个过程需要通过交换机（exchange）来进行。但是为了不让细节拖累我们的进度，这里我们只需要知道如何使用由空字符串表示的默认交换机即可。如果你想要详细了解交换机
"""

"""
参数说明
exchange 交换机
routing_key 队列的名称
body  消息的主体
"""
channel.basic_publish(exchange='',routing_key="hello",body="Hollo xiaowen!")

print(" [x] Sent 'Hello xiaowen!'")

"""
发送不成功！
如果这是你第一次使用RabbitMQ，并且没有看到“Sent”消息出现在屏幕上，你可能会抓耳挠腮不知所以。这也许是因为没有足够的磁盘空间给代理使用所造成的（代理默认需要200MB的空闲空间），所以它才会拒绝接收消息。查看一下代理的日志文件进行确认，如果需要的话也可以减少限制。配置文件文档会告诉你如何更改磁盘空间限制（disk_free_limit）。
"""
connection.close()
