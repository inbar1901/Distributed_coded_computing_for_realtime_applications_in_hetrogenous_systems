import pika
import sys
import os


def main():
    # establish a connection with RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # creating recipient queue
    channel.queue_declare(queue='try')

    # define a callback method for when receiving a message
    def callback(ch, method, properties, body):
        print(f' [x] Received {body.decode()}')

    # receive messages from 'Try' queue
    channel.basic_consume(queue='try', auto_ack=True, on_message_callback=callback)

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
