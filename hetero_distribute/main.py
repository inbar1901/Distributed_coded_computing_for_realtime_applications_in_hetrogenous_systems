import uuid

import pika
import sys
import time
import os
import json
import numpy as np
# Adding a file from the nfs that contains all the ips, usernames and passwords
sys.path.append("/var/nfs/general") # from computer
# sys.path.append("/nfs/general") # from servers
import cred

local_nfs_path = "/var/nfs/general"
server_nfs_path = "/nfs/general"

class Producer(object):
    def __init__(self):
        #############################################
        # creates the queue used to deliver work to workers
        #############################################

        # establish a connection with RabbitMQ server
        # using our vhost named 'proj_host' in IP <cred.pc_ip> and port 5672
        self.credentials = pika.PlainCredentials(cred.rbt_user, cred.rbt_password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(cred.pc_ip, 5672, cred.rbt_vhost, self.credentials))
        self.channel = self.connection.channel()

        # creating an exchange
        self.exchange_name = 'my_exchange'
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # where to run
        valid_run_type = 1
        while valid_run_type:
            self.run_type = str(input('where to run: ')) # s - server, l - local
            if self.run_type == "s":
                self.path = server_nfs_path
                valid_run_type = 0
            elif self.run_type == "l":
                self.path = local_nfs_path
                valid_run_type = 0

        # create a message queue
        self.message_queue_name = str(input('enter name of queue: '))
        # print("[init]: queue name:" + self.message_queue_name) # FOR DEBUG
        self.to_worker_num = int(self.message_queue_name[1])
        print(f'[init]: worker number: {self.to_worker_num}') # FOR DEBUG
        result = self.channel.queue_declare(queue=self.message_queue_name)  # message queue = sends work to workers
        self.fb_queue_name = result.method.queue

        # connect exchange and message queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.message_queue_name)

        # receive message
        self.channel.basic_consume(queue=self.fb_queue_name, on_message_callback=self.worker_response_to_producer, auto_ack=True)

        # job managing
        self.curr_job_number = 0

    def worker_response_to_producer(self, ch, method, props, body):
        #############################################
        # On response arrival, if the sender is identified as the worker we are waiting for,
        # push the response to the producer
        # params: props -       message queue properties
        #         body -        response content
        #############################################
        print("[worker_response_to_producer]: begin") # FOR DEBUG
        print("[worker_response_to_producer]: self.corr_id: " + str(self.corr_id)) # FOR DEBUG
        print("[worker_response_to_producer]: props.correlation_id: " + str(props.correlation_id)) # FOR DEBUG
        # if self.corr_id == props.correlation_id:
        #     self.response = body

        if "[v] from worker" in str(body):
            self.response = body

    def send_task_wait_for_feedback(self):
        #############################################
        # Send a job to a worker and wait for response
        #############################################
        print("[wait_for_feedback]: begin") # FOR DEBUG

        self.response = None
        self.corr_id = str(uuid.uuid4())

        # reading the jobs from a file
        jobs_file_name = str(input('enter name of json file: '))
        with open(f'./{jobs_file_name}') as f:
            job = str(json.load(f))

        # create a dictionary: splitting the job to tasks + adding headers
        k = 2 # TODO - implementing optimization problem to determine k
        header = self.create_header(3,1,k) # todo - add loops for all tasks in job and for all jobs

        # write the task to a file in the nfs
        work_file_name = f'{local_nfs_path}/w{self.to_worker_num}_job{header["job_number"]}_task{header["task_number"]}'
        with open(work_file_name, 'w') as f_work:
            json.dump(job, f_work)

        task_file_for_servers = f'{self.path}/w{self.to_worker_num}_job{header["job_number"]}_task{header["task_number"]}'
        task = str({"Header": header, "task": task_file_for_servers})

        print(f' [x] Sent task: ' + task)

        # connecting to channel
        self.corr_id = str(np.random.rand())
        print("[wait_for_feedback]: corr_id: " + str(self.corr_id)) # FOR DEBUG

        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.message_queue_name,
                                   properties=pika.BasicProperties(reply_to=self.fb_queue_name, correlation_id=self.corr_id),
                                   body=task)

        # waiting for response
        while self.response is None:
            self.connection.process_data_events()
        print(f' [V] received response: ' + self.response.decode())

        # close connection
        self.connection.close()
        return self.response



    def create_header(self, job_num, task_num, k):
        """
        create message header with the details:
        :param job_num: job number
        :param task_num: task number
        :param k: amount of tasks to complete job
        :return: header for one task
        """
        header = {"job_number": job_num, "task_number": task_num, "k": k}
        return header

def main():
    producer = Producer()
    res = producer.send_task_wait_for_feedback()


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









