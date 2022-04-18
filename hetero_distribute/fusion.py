#!/usr/bin/env python3
import pika
import sys
import os
import time
import json
import ast

# Adding a file from the nfs that contains all the ips, usernames and passwords
sys.path.append("/var/nfs/general") # from computer
# sys.path.append("nfs/general/cred.py") # from servers

import cred


class Fusion():
    def __init__(self):
        # establish a connection with RabbitMQ server
        # using our vhost named 'proj_host' in IP <cred.pc_ip> and port 5672
        self.credentials = pika.PlainCredentials(cred.rbt_user, cred.rbt_password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(cred.pc_ip, 5672, cred.rbt_vhost, self.credentials))
        self.channel = self.connection.channel()

        # creating an exchange
        self.exchange_name = 'fusion_exchange'
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # worker 1 ----------------------------------------------------------------------
        # create a message queue
        worker1_queue_name = "w1_fusion"
        self.producer1_channel = self.channel.queue_declare(queue=worker1_queue_name)

        # receive task result from workers
        self.channel.basic_consume(queue=worker1_queue_name, on_message_callback=self.receive_results)

        # connect exchange and message queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=worker1_queue_name)

        # worker 2 ----------------------------------------------------------------------
        # create a message queue
        worker2_queue_name = "w2_fusion"
        self.producer1_channel = self.channel.queue_declare(queue=worker2_queue_name)

        # receive task result from workers
        self.channel.basic_consume(queue=worker2_queue_name, on_message_callback=self.receive_results)

        # connect exchange and message queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=worker2_queue_name)

        # worker 3 ----------------------------------------------------------------------
        # create a message queue
        worker3_queue_name = "w3_fusion"
        self.producer1_channel = self.channel.queue_declare(queue=worker3_queue_name)

        # receive task result from workers
        self.channel.basic_consume(queue=worker3_queue_name, on_message_callback=self.receive_results)

        # connect exchange and message queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=worker3_queue_name)

        # consuming ---------------------------------------------------------------------
        # start listening for task results
        print(' [*] waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def receive_results(self, ch, method, properties, body):
        print(' [x] Received')
        print(' [x] Converting into json file')
        # convert message into json file and save it
        msg = ast.literal_eval(json.dumps(body.decode()))

        out_file = open('out.json', 'w')
        out_file.write(msg)
        out_file.close()
        print(' [x] Saved json file')

        # sending feedback to producer
        response = ' [v] from fusion: received work'
        # print("[receive_results]: properties.reply_to: " + str(properties.reply_to) + " type: " + str(
        #     type(properties.reply_to))) # FOR DEBUG
        # print("[receive_results]: properties.correlation_id: " + str(properties.correlation_id) ) # FOR DEBUG
        exchange_name = 'fusion_exchange' # double declaration
        ch.basic_publish(exchange=exchange_name, routing_key=str(properties.reply_to),
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=str(response))

        print(' [x] Done')


def main():
    fusion = Fusion()



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
