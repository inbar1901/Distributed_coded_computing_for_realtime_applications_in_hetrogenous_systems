import pika
import sys
import os
import time


def main():
    # establish a connection with RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # creating an exchange
    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    # creating recipient queue
    result = channel.queue_declare(queue='', exclusive=True)

    # connect exchange and queue
    queue_name = result.method.queue
    channel.queue_bind(exchange='logs', queue=queue_name)

    # define a callback method for when receiving a message
    def callback(ch, method, properties, body):
        print(f' [x] Received {body.decode()}')
        time.sleep(body.count(b'.'))
        print(' [x] Done')
        ch.basic_ack(delivery_tag=method.delivery_tag)  # acknowledging getting the message

    # receive messages from 'Try' queue
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    # start listening to messages
    print(' [*] waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


# define never-ending loop that waits for data
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupt")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
