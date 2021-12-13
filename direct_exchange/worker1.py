import pika
import sys
import os
import time
import json

def main():
    # establish a connection with RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # creating an exchange
    exchange_name = 'my_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    # creating recipient queue
    queue_name = 'worker1'
    result = channel.queue_declare(queue=queue_name)

    # connect exchange and queue
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    # define a callback method for when receiving a message
    def callback(ch, method, properties, body):
        print(' [x] Received')
        print(' [x] Converting into json file')
        # convert message into json file and save it
        msg = json.dumps(body.decode())
        out_file = open('out.json', 'w')
        out_file.write(msg)
        out_file.close
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
