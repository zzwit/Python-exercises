import pika

# 实例化 Mq
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
#生成的句柄
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)

channel.basic_consume(callback,queue="task_queue")


print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
