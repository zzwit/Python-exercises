import pika


"""

将会从队列中获取消息并将其打印到屏幕上。

"""

# 链接的服务
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
#生产的句柄
channel = connection.channel()

"""
我们需要确认队列是存在的。我们可以多次使用queue_declare命令来创建同一个队列，但是只有一个队列会被真正的创建。

你也许要问: 为什么要重复声明队列呢 —— 我们已经在前面的代码中声明过它了。如果我们确定了队列是已经存在的，那么我们可以不这么做，比如此前预先运行了send.py程序。可是我们并不确定哪个程序会首先运行。这种情况下，在程序中重复将队列重复声明一下是种值得推荐的做法。

"""
channel.queue_declare(queue='hello')


def callback(ch,method,properties,body):
    print(" [x] Received %r" % body)


channel.basic_consume(callback,queue="hello",no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

"""
成功了！我们已经通过RabbitMQ发送第一条消息。你也许已经注意到了，receive.py程序并没有退出。它一直在准备获取消息，你可以通过Ctrl-C来中止它。
"""
