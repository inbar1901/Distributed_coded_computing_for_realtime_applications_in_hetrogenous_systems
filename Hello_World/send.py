import pika


# establish a connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# creating recipient queue
channel.queue_declare(queue='try')

# send a message threw exchange
channel.basic_publish(exchange='', routing_key='try', body='Hello world!')
print(' [x] message sent')

# close connection
connection.close()

