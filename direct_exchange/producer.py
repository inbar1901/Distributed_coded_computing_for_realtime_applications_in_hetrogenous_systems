import pika
import sys
import time
import json


# establish a connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# creating an exchange
exchange_name = 'my_exchange'
channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

# create a queue
queue_name = str(input('enter name of queue: '))
result = channel.queue_declare(queue=queue_name)

# connect exchange and queue
channel.queue_bind(exchange=exchange_name, queue=queue_name)

# setting the body of the message
# message = ''.join(sys.argv[1:]) or 'Hi'
file_name = str(input('enter name of json file: '))
with open(f'./{file_name}') as f:
    message = str(json.load(f))

# connecting to channel
channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=message)
print(f' [x] Sent {message}')

# close connection
connection.close()







