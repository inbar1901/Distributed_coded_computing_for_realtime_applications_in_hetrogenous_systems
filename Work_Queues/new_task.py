import pika
import sys
import time


# establish a connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# creating recipient queue
channel.queue_declare(queue='try')

# setting the body of the message
message = ''.join(sys.argv[1:]) or 'Hi'



# connecting to channel
channel.basic_publish(exchange='', routing_key='try', body=message)
print(f' [x] Sent {message  }')

# close connection
connection.close()







