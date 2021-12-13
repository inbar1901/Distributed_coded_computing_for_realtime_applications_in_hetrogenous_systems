import pika
import sys
import time
import json
import numpy as np


# establish a connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# creating an exchange
exchange_name = 'my_exchange'
channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

# ---------------------------------------------------------------
# create a queue
queue_name = str(input('enter name of queue: '))
result = channel.queue_declare(queue=queue_name)

# connect exchange and queue
channel.queue_bind(exchange=exchange_name, queue=queue_name)

# ---------------------------------------------------------------
# create a feedback queue
fb_queue_name = 'producer_feedback'
fb_result = channel.queue_declare(queue=fb_queue_name)


def fb_callback(ch, method, properties, body):
    print(f'{properties.correlation_id}')
    #  getting a response from the worker
    print(f' [x] We got a response: {body.decode()}')


# receive message
channel.basic_consume(queue=fb_queue_name, on_message_callback=fb_callback, auto_ack=True)

# ---------------------------------------------------------------
# setting the body of the message
# message = ''.join(sys.argv[1:]) or 'Hi'
file_name = str(input('enter name of json file: '))
with open(f'./{file_name}') as f:
    message = str(json.load(f))

# connecting to channel
corr_id = str(np.random.rand())
channel.basic_publish(exchange=exchange_name, routing_key=queue_name,
                      properties=pika.BasicProperties(reply_to=fb_queue_name, correlation_id=corr_id), body=message)
print(f' [x] Sent {message}')

time.sleep(10)

# close connection
connection.close()







