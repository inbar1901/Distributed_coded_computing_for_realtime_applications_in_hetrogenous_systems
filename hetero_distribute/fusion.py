#!/usr/bin/env python3
import pika
import sys
import os
import time
import json
import ast
import collections

# Adding a file from the nfs that contains all the ips, usernames and passwords
sys.path.append("/var/nfs/general") # from computer
sys.path.append("/nfs/general") # from servers

import our_cred_ig

amount_of_workers = 5

local_nfs_path = "/var/nfs/general"
server_nfs_path = "/nfs/general"

class Fusion():
    def __init__(self):
        ###### ------------ fusion variables init ------------ ######
        self.count_tasks = {}
        self.forbidden_job_mat = {}
        for i in range(amount_of_workers):
            self.forbidden_job_mat[str(i+1)]=[]
        self.first_task_flag = True # indicates that we got the first task. Used for reading data from main
        self.jobs_amount = None
        self.start_time = None

        ###### ------- establish a connection with RabbitMQ server ------- ######
        # using our vhost named 'proj_host' in IP <cred.pc_ip> and port 5672
        self.credentials = pika.PlainCredentials(our_cred_ig.rbt_user, our_cred_ig.rbt_password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(our_cred_ig.pc_ip, 5672, our_cred_ig.rbt_vhost, self.credentials))
        self.channel = self.connection.channel()

        # creating an exchange
        self.exchange_name = 'fusion_exchange'
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # declares
        self.message_queue_names = {}
        self.worker_to_fusion_res = {}
        #self.feedback_queue_names = {}
        #self.fb_fusion_to_worker_res = {}

        for i in range(1, amount_of_workers+1):
            # declare the worker->fusion work queues
            self.message_queue_names[i] = "w" + str(i) + "_fusion"
            self.worker_to_fusion_res[i] = self.channel.queue_declare(
                queue=self.message_queue_names[i])  # message queue = sends work to workers

            # declare the fusion->worker feedback queues
            #self.feedback_queue_names[i] = "fb_w" + str(i) + "_fusion"
            #self.fb_fusion_to_worker_res[i] = self.channel.queue_declare(queue=self.feedback_queue_names[i])

            # setting the feedback queue we declared as the feedback queue of the worker->fusion queue
            #self.worker_to_fusion_res[i].method.queue = self.feedback_queue_names[i]

            # connect exchange and message (worker->fusion) queue
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.message_queue_names[i])

            # connect exchange and message (fusion->worker) queue
            #self.channel.queue_bind(exchange=self.exchange_name, queue=self.feedback_queue_names[i])

            # receive task result from workers
            self.channel.basic_consume(queue=self.message_queue_names[i], on_message_callback=self.receive_results)


        ######################################################################################
        """# worker 1 ----------------------------------------------------------------------
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
        self.channel.queue_bind(exchange=self.exchange_name, queue=worker3_queue_name)"""
        ##########################################################################

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

        # convert message into json file and save it
        msg = ast.literal_eval(json.dumps(body.decode()))

        # count the received task in the right job
        dict_msg = ast.literal_eval(msg)  # we need DOUBLE unpacking because we have double casting to str
        print(" [receive_results] The result received from worker: " + str(dict_msg)) # FOR DEBUG
        msg_header = dict_msg["Header"]
        worker_num = msg_header["worker_num"]
        task_path = dict_msg["result"]
        self.nfs_path = os.path.dirname(task_path)

        # Read data from main if it's the first task
        if self.first_task_flag:
            self.first_task_flag = False
            fusion_file = "data_for_fusion"
            self.data_from_main_path = f'{self.nfs_path}/{fusion_file}'
            with open(self.data_from_main_path, 'r') as f:
                data = f.read()
            data = ast.literal_eval(data)
            self.jobs_amount = data["jobs_amount"]
            self.start_time = data["start_time"]

        # we want to make sure we did not receive a "leftover" task of one of the finished jobs
        # a "leftover" is a task from an already finished job - happens only when a worker has not yet been notified
        # the job was finished. once all workers were notified the job was finished, no such "leftover" task is possible
        if msg_header["job_number"] in self.forbidden_job_mat[str(worker_num)]:
            print(f' [x] The job {msg_header["job_number"]} is already done\n\n')
        else:
            # count the received task in the right job
            self.count_tasks_func(msg_header)
            self.count_tasks = dict(collections.OrderedDict(sorted(self.count_tasks.items())))
            print("[receive_results]: count_tasks: " + str(self.count_tasks)) # FOR DEBUG

            # save result
            out_file_path = os.path.join(self.nfs_path, 'out')
            with open(out_file_path, 'w') as f_out_file:
                f_out_file.write(json.dumps(msg))

            print(' [x] Saved json file')

        # Delete tasks file from nfs
        os.remove(task_path)

        # sending feedback to producer (worker)
        response = {"checksum": ' [v] from fusion: ', "forbidden_jobs": self.forbidden_job_mat[str(worker_num)]}

        exchange_name = 'fusion_exchange' # double declaration
        ch.basic_publish(exchange=exchange_name, routing_key=str(properties.reply_to),
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=str(response))

        print(' [x] Done\n\n')

        # after response sent, we delete worker i's forbidden jobs
        self.update_forbidden_jobs(worker_num)

        # checks if we got all jobs done already
        if self.jobs_amount == 0:
            finish_time = time.time()
            work_time = finish_time - self.start_time
            print("[receive_results]: finished working. work time: " + str(work_time) )

            with open(self.data_from_main_path, 'w') as f:
                f.write(json.dumps(work_time))

    def count_tasks_func(self, msg_header):
        """
        Count_tasks is a dictionary of job numbers
        in each job nuber we have a list: max tasks needed = k | current tasks received
        :param msg_header: current message header
        """
        if str(msg_header["job_number"]) in self.count_tasks.keys(): # Not the first task for that job
            self.count_tasks[str(msg_header["job_number"])][1] += 1
            # if we received enough tasks for the job
            if self.count_tasks[str(msg_header["job_number"])][1]==self.count_tasks[str(msg_header["job_number"])][0]:
                self.purge_queues_func(msg_header["job_number"])
                self.jobs_amount -= 1 # update remaining jobs amount
        else: # The first task for that job
            self.count_tasks[str(msg_header["job_number"])] = [msg_header["k"], 1]

    def purge_queues_func(self, job_num):
        """
        For each worker, we append the ended job to the forbidden job list (needed to be sent to workers for update)
        :param job_num: current job to add to forbidden jobs matrix
        """
        self.count_tasks.pop(str(job_num))
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
