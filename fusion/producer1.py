import uuid
import pika
import sys
import time
import os
import json
import numpy as np


class Producer(object):
    def __init__(self):
        #############################################
        # creates the queue used to deliver work to workers
        #############################################

        # establish a connection with RabbitMQ server
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # creating an exchange
        self.exchange_name = 'fusion_exchange'
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # create a message queue
        self.message_queue_name = "working_producer_1"
        result = self.channel.queue_declare(queue=self.message_queue_name)  # message queue = sends work to workers
        self.fb_queue_name = result.method.queue

        # connect exchange and message queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.message_queue_name)

        # receive message
        self.channel.basic_consume(queue=self.fb_queue_name, on_message_callback=self.fusion_response, auto_ack=True)

    def fusion_response(self, ch, method, props, body):
        #############################################
        # on response arrival, if the sender is identified as the worker we are waiting for,
        # push the response to the producer
        # params: props -       message queue properties
        #         body -        response content
        #############################################
        if self.corr_id == props.correlation_id:
            self.response = body

    def wait_for_feedback(self):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        # setting the task as body of the message
        file_name = str(input('enter name of json file: '))
        with open(f'./{file_name}') as f:
            task = str(json.load(f))
        print(f' [x] Sent {task}')

        # connecting to channel
        self.corr_id = str(np.random.rand())
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.message_queue_name,
                                   properties=pika.BasicProperties(reply_to=self.fb_queue_name, correlation_id=self.corr_id),
                                   body=task)

        # waiting for response
        while self.response is None:
            self.connection.process_data_events()

        time.sleep(10)
        print(" [x] Got an answer")

        # close connection
        self.connection.close()
        return self.response


def main():
    producer = Producer()
    res = producer.wait_for_feedback()


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










