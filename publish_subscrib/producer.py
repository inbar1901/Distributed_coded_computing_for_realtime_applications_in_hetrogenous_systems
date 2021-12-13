import pika
import sys
import time


# establish a connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# creating an exchange
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# create a queue
result = channel.queue_declare(queue='', exclusive=True)

# connect exchange and queue
queue_name = result.method.queue
channel.queue_bind(exchange="logs", queue=queue_name)

# setting the body of the message
message = ''.join(sys.argv[1:]) or 'Hi'

# connecting to channel
channel.basic_publish(exchange='logs', routing_key='', body=message)
print(f' [x] Sent {message}')

# close connection
connection.close()







