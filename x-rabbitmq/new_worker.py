import pika
import time
"""
 工作队列 
 
 消息确认
    当处理一个比较耗时得任务的时候，你也许想知道消费者（consumers）是否运行到一半就挂掉。当前的代码中，当消息被RabbitMQ发送给消费者（consumers）之后，马上就会在内存中移除。这种情况，你只要把一个工作者（worker）停止，正在处理的消息就会丢失。同时，所有发送到这个工作者的还没有处理的消息都会丢失。
    
    我们不想丢失任何任务消息。如果一个工作者（worker）挂掉了，我们希望任务会重新发送给其他的工作者（worker）。
    
    为了防止消息丢失，RabbitMQ提供了消息响应（acknowledgments）。消费者会通过一个ack（响应），告诉RabbitMQ已经收到并处理了某条消息，然后RabbitMQ就会释放并删除这条消息。
    
    如果消费者（consumer）挂掉了，没有发送响应，RabbitMQ就会认为消息没有被完全处理，然后重新发送给其他消费者（consumer）。这样，及时工作者（workers）偶尔的挂掉，也不会丢失消息。
    
    消息是没有超时这个概念的；当工作者与它断开连的时候，RabbitMQ会重新发送消息。这样在处理一个耗时非常长的消息任务的时候就不会出问题了。
    
    消息响应默认是开启的。之前的例子中我们可以使用no_ack=True标识把它关闭。是时候移除这个标识了，当工作者（worker）完成了任务，就发送一个响应。
"""
# 实例化 Mq
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
#生产的句柄
channel = connection.channel()

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(callback,queue="hello")


print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
