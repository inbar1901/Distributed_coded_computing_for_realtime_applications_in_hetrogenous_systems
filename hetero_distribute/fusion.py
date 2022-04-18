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

amount_of_workers = 3

class Fusion():
    def __init__(self):
        ###### ------------ fusion variables init ------------ ######
        self.count_tasks = {}
        self.forbidden_job_mat = {}
        for i in range(amount_of_workers):
            self.forbidden_job_mat[str(i+1)]=[]

        ###### ------- establish a connection with RabbitMQ server ------- ######
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
        """
        called whenever fusion gets a result from a worker
        :param ch: channel
        :param method: --
        :param properties: current message properties
        :param body: msg content
        """
        print(' [x] Received')
        print(' [x] Converting into json file')
        # convert message into json file and save it
        msg = ast.literal_eval(json.dumps(body.decode()))

        # count the received task in the right job
        dict_msg = ast.literal_eval(msg) # we need DOUBLE unpacking because we have double casting to str
        msg_header = dict_msg["Header"]
        self.count_tasks_func(msg_header)
        print(str(self.count_tasks)) # FOR DEBUG

        # save result
        out_file = open('out.json', 'w')
        out_file.write(msg)
        out_file.close()
        print(' [x] Saved json file')

        worker_num = msg_header["worker_num"]

        # sending feedback to producer
        response = {"checksum": ' [v] from fusion: ', "forbidden_jobs": self.forbidden_job_mat[str(worker_num)]}

        exchange_name = 'fusion_exchange' # double declaration
        ch.basic_publish(exchange=exchange_name, routing_key=str(properties.reply_to),
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=str(response))

        print(' [x] Done\n\n')

        # after response sent, we delete worker i's forbidden jobs
        self.update_forbidden_jobs(worker_num)

    def count_tasks_func(self, msg_header):
        """
        count_tasks is a dictionary of job numbers
        in each job nuber we have a list: max tasks needed = k | current tasks received
        :param msg_header: current message header
        """
        if str(msg_header["job_number"]) in self.count_tasks.keys():
            self.count_tasks[str(msg_header["job_number"])][1] += 1
            # if we received enough tasks for the job
            if self.count_tasks[str(msg_header["job_number"])][1]==self.count_tasks[str(msg_header["job_number"])][0]:
                self.purge_queues_func(msg_header["job_number"])
        else:
            self.count_tasks[str(msg_header["job_number"])] = [msg_header["k"], 1]

    def purge_queues_func(self, job_num):
        """
        for each worker, we append the ended job to the forbidden job list (needed to be sent to workers for update)
        :param job_num: current job to add to forbidden jobs matrix
        """
        for i in range(amount_of_workers):
            self.forbidden_job_mat[str(i+1)].append(job_num)

    def update_forbidden_jobs(self, worker_num):
        """
        after we sent the forbidden job to the worker
        we no longer need to remember it
        :param worker_num: worker to clean it's forbidden job matrix
        """
        self.forbidden_job_mat[str(worker_num)] = []

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
