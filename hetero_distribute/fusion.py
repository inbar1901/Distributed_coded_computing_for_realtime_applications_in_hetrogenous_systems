import pika
import sys
import os
import time
import json


def receive_results(ch, method, properties, body):
    print(' [x] Received')
    print(' [x] Converting into json file')
    # convert message into json file and save it
    msg = json.dumps(body.decode())
    out_file = open('out.json', 'w')
    out_file.write(msg)
    out_file.close()
    print(' [x] Saved json file')

    # sending feedback to producer
    response = ' [v] worker1 is done'
    print("[receive_results]: properties.reply_to: " + str(properties.reply_to) + " type: " + str(
        type(properties.reply_to)))
    print("[receive_results]: properties.correlation_id: " + str(properties.correlation_id) )
    exchange_name = 'fusion_exchange' # double declaration
    ch.basic_publish(exchange=exchange_name, routing_key=str(properties.reply_to),
                     properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=str(response))

    ch.basic_ack(delivery_tag=method.delivery_tag)  # acknowledging getting the message
    print(' [x] Done')


def main():
    # establish a connection with RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # creating an exchange
    exchange_name = 'fusion_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    # worker 1 ----------------------------------------------------------------------
    # create a message queue
    worker1_queue_name = "w1_fusion"
    producer1_channel = channel.queue_declare(queue=worker1_queue_name)

    # receive task result from workers
    channel.basic_consume(queue=worker1_queue_name, on_message_callback=receive_results)

    # worker 2 ----------------------------------------------------------------------
    # create a message queue
    worker2_queue_name = "w2_fusion"
    producer1_channel = channel.queue_declare(queue=worker2_queue_name)

    # receive task result from workers
    channel.basic_consume(queue=worker2_queue_name, on_message_callback=receive_results)

    # worker 3 ----------------------------------------------------------------------
    # create a message queue
    worker3_queue_name = "w3_fusion"
    producer1_channel = channel.queue_declare(queue=worker3_queue_name)

    # receive task result from workers
    channel.basic_consume(queue=worker3_queue_name, on_message_callback=receive_results)

    # consuming ---------------------------------------------------------------------
    # start listening for task results
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
