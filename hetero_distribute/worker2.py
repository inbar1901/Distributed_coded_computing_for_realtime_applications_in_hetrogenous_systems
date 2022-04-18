import pika
import sys
import os
import time
import json
import uuid
import numpy as np
import ast

# Adding a file from the nfs that contains all the ips, usernames and passwords
sys.path.append("/var/nfs/general") # from computer
# sys.path.append("/nfs/general/cred.py") # from servers

import cred

class Worker(object):
    def __init__(self):
        # establish a connection with RabbitMQ server
        # using our vhost named 'proj_host' in IP <cred.pc_ip> and port 5672
        self.credentials = pika.PlainCredentials(cred.rbt_user, cred.rbt_password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(cred.pc_ip, 5672, cred.rbt_vhost, self.credentials))
        self.channel = self.connection.channel()

        # --------------------- parameters for main node --------------------- #
        # creating an exchange with main node
        self.main_exchange_name = 'main_exchange'
        self.channel.exchange_declare(exchange=self.main_exchange_name, exchange_type='direct')

        # creating recipient queue from main node
        self.worker_queue_name = 'w2_main'
        self.channel.queue_declare(queue=self.worker_queue_name)

        # connect exchange and queue
        self.channel.queue_bind(exchange=self.main_exchange_name, queue=self.worker_queue_name)

        # --------------------- parameters for fusion node --------------------- #
        # establish a connection with RabbitMQ server
        # using our vhost named 'proj_host' in IP <cred.pc_ip> and port 5672
        self.connection_fusion = pika.BlockingConnection(pika.ConnectionParameters(cred.pc_ip, 5672, cred.rbt_vhost, self.credentials))
        self.channel_fusion = self.connection_fusion.channel()

        # creating an exchange with fusion node
        self.fusion_exchange_name = 'fusion_exchange'
        self.channel_fusion.exchange_declare(exchange=self.fusion_exchange_name, exchange_type='direct')

        # create fusion queues
        self.fusion_queue_name = "w2_fusion"
        self.fusion_queue_declare = self.channel_fusion.queue_declare(queue=self.fusion_queue_name)
        self.fusion_feedback_queue_name = str(self.fusion_queue_declare.method.queue)
        # print("[init]: fusion feedback queue name: " + self.fusion_feedback_queue_name + " type: " + str(type(self.fusion_feedback_queue_name))) # FOR DEBUG

        # connect exchange and fusion queue
        self.channel_fusion.queue_bind(exchange=self.fusion_exchange_name, queue=self.fusion_queue_name)

        # connect feedback from fusion node
        self.channel_fusion.basic_consume(queue=self.fusion_feedback_queue_name, on_message_callback=self.fusion_response,
                                   auto_ack=True)

        # --------------------- consume from all queues --------------------- #
        # consumer - worker from main node #
        # receive messages from main node
        self.channel.basic_consume(queue=self.worker_queue_name, on_message_callback=self.work)

        # start listening for messages
        print(' [*] waiting for tasks. To exit press CTRL+C')
        self.channel.start_consuming()



    def fusion_response(self, ch, method, props, body):
        #############################################
        # on response arrival, if the sender is identified as the worker we are waiting for,
        # push the response to the producer
        # params: props -       message queue properties
        #         body -        response content
        #############################################
        print("[fusion_response]: begin") # FOR DEBUG
        print("[fusion_response]: self.corr_id: " + str(self.corr_id)) # FOR DEBUG
        print("[fusion_response]: props.correlation_id: " + str(props.correlation_id)) # FOR DEBUG
        print("[fusion_response]: body: " + str(body)) # FOR DEBUG

        if "[v] from fusion" in str(body):
            print("[fusion_response]: inside if")  # FOR DEBUG
            self.response_from_fusion = body

        # if self.corr_id == props.correlation_id:
        #     print("[fusion_response]: inside if") # FOR DEBUG
        #     self.response_from_fusion = body

    def send_to_fusion_and_wait_for_feedback(self):
        #############################################
        # send the task result to fusion node and wait for response
        #############################################
        print("[send_to_fusion_and_wait_for_feedback]: begin") # FOR DEBUG
        print("[send_to_fusion_and_wait_for_feedback]: queue name: " + str(self.fusion_feedback_queue_name)) # FOR DEBUG


        # init
        self.response_from_fusion = None

         # send to fusion
        self.corr_id = str(np.random.rand())
         # FOR DEBUG
        print("[send_to_fusion_and_wait_for_feedback]: exchange name: " + str(self.fusion_exchange_name) + " fusion_queue_name: " + str(self.fusion_queue_name) + " result: " +str(self.result)) # FOR DEBUG
        self.channel_fusion.basic_publish(exchange=self.fusion_exchange_name, routing_key=self.fusion_queue_name,
                                             properties=pika.BasicProperties(reply_to=self.fusion_feedback_queue_name,
                                                                             correlation_id=self.corr_id), body=str(self.result))
        print(f' [x] Sent task to fusion')
        # print("[send_to_fusion_and_wait_for_feedback]: sent result to fusion") # FOR DEBUG

        # waiting for response from fusion
        while self.response_from_fusion is None:
            self.connection_fusion.process_data_events()

        print(" [x] Got an answer from fusion")


    def work(self, ch, method, properties, body):
        #############################################
        # getting work from main, executing task and
        # send response to main
        #############################################
        print(' [x] Received task from main')
        # init variables
        self.result = None

        # convert message into json file and save it
        task = ast.literal_eval(json.dumps(body.decode()))
        task = ast.literal_eval(task) # we need DOUBLE unpacking

        # read task
        header,task_content = self.upack_task(task)

        # compute result
        task_content = self.multiply_polynomes(task_content)
        self.result = {"Header": header, "result": task_content}

        # write answer
        task_result_file_name = 'w2.json'
        out_file = open(task_result_file_name, 'w')
        out_file.write(json.dumps(self.result))
        out_file.close()
        print(' [x] Saved json file')

        # sending feedback to main
        response_to_main = ' [v] from worker: worker1 is done'
        # print("[work]: responding to main") # FOR DEBUG

        ch.basic_publish(exchange=self.main_exchange_name, routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=str(response_to_main))
        # print("[work]: sent response to main") # FOR DEBUG

        # sending task result to fusion node
        self.send_to_fusion_and_wait_for_feedback()
        print(' [x] Done\n\n')

        return

    def upack_task(self, task):
        """

        """
        header = task["Header"]
        task_content = task["task"]
        return header, task_content

    def multiply_polynomes(self, task_content):
        """

        """
        time.sleep(1) # TODO change to simulation results
        return task_content


def main():
    worker = Worker()
    # close connection
    # self.connection.close()
    # self.connection_fusion.close()



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