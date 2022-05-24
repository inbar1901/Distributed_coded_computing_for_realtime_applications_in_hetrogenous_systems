import uuid

import pika
import sys
import time
import os
import json
import numpy as np
import threading
import ast

# Adding a file from the nfs that contains all the ips, usernames and passwords
sys.path.append("/var/nfs/general") # from computer
# sys.path.append("/nfs/general") # from servers
import cred

local_nfs_path = "/var/nfs/general"
server_nfs_path = "/nfs/general"

num_of_workers_in_system = 3 # TODO set to real amount

class Producer(object):
    internal_lock = threading.Lock()

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

        # create all message queues and bind them:
        #   1. main -> worker queues
        #   2. worker -> main feedback queues

        # declares
        self.message_queue_names = {}
        self.main_to_worker_res = {}
        self.feedback_queue_names = {}
        self.fb_worker_to_main_res = {}

        for i in range(1, num_of_workers_in_system+1):
            # declare the main->worker work queues
            self.message_queue_names[i] = "w" + str(i) + "_main"
            self.main_to_worker_res[i] = self.channel.queue_declare(
                queue=self.message_queue_names[i])  # message queue = sends work to workers

            # declare the worker->main feedback queues
            self.feedback_queue_names[i] = "fb_w" + str(i) + "_main"
            self.fb_worker_to_main_res[i] = self.channel.queue_declare(queue=self.feedback_queue_names[i])

            # setting the feedback queue we declared as the feedback queue of the main->worker queue
            self.main_to_worker_res[i].method.queue = self.feedback_queue_names[i]

            # connect exchange and message (main->worker) queue
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.message_queue_names[i])

            # connect exchange and message (main->worker) queue
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.feedback_queue_names[i])


        # default - start sending tasks to the first worker and so on
        self.to_worker_num = 1
        # print(f'[init]: worker number: {self.to_worker_num}') # FOR DEBUG

        # job managing
        self.curr_job_number = 0
        self.not_finished = None # flag that indicates if all tasks were sent to workers
        self.connection_up = False
        self.free_workers = [worker_record(worker_num) for worker_num in range(1, num_of_workers_in_system+1)]
        self.busy_workers = []

        # create thread for reading workers messages
        self.listen_workers_thread = threading.Thread(target=self.listen_to_workers)
        self.listen_workers_thread.setDaemon(True)
        self.listen_workers_thread.start()

        # Average time list for all workers
        self.average_workers_time = list(np.zeros(num_of_workers_in_system+1))

        # Wait for thread to connect
        while self.connection_up == False:
            pass

    def listen_to_workers(self):
        """

        :return:
        """
        print("[listen_to_workers]: begin") # FOR DEBUG
        # wait for messages in the feedback queue
        for i in range(1,num_of_workers_in_system+1):
            self.channel.basic_consume(queue=self.feedback_queue_names[i], on_message_callback=self.worker_response_to_producer, auto_ack=False)

        self.connection_up = True
        while self.not_finished is None:
            with self.internal_lock:
                # waiting for response from workers
                self.connection.process_data_events()


    def worker_response_to_producer(self, ch, method, props, body):
        #############################################
        # On response arrival, if the sender is identified as the worker we are waiting for,
        # push the response to the producer
        # params: props -       message queue properties
        #         body -        response content
        #############################################
        print("[worker_response_to_producer]: begin") # FOR DEBUG
        body = body.decode()

        if " [v] from worker" in str(body):
            print("[worker_response_to_producer]: " + str(body))  # FOR DEBUG
            self.response = ast.literal_eval(body)
            self.average_workers_time[self.response["worker_num"]] = self.response["average_time"]
            print("[worker_response_to_producer]: average_workers_time: " + str(self.average_workers_time[1:]))  # FOR DEBUG


    def send_task_wait_for_feedback(self):
        #############################################
        # Send a job to a worker and wait for response
        #############################################
        print("[wait_for_feedback]: begin") # FOR DEBUG

        self.corr_id = str(uuid.uuid4())

        # reading the jobs from a file
        jobs_file_name = "./jobs.json"
        with open(jobs_file_name) as f:
            self.all_jobs = json.load(f)

        # Create a file for fusion
        self.data_for_fusion()

        for curr_job_num in self.all_jobs.keys(): # reading jobs one by one from jobs_file
            # create a dictionary: splitting the job to tasks + adding headers
            k = self.divide_job_into_tasks(curr_job_num)

            for curr_task_num in range(1,k+1):
                self.response = None
                header = self.create_header(int(curr_job_num),curr_task_num,k)

                task_file_for_servers = f'{self.path}/job{header["job_number"]}_task{header["task_number"]}'
                task = str({"Header": header, "task": task_file_for_servers})

                print(f' [x] Sent task: ' + task)

                # connecting to channel
                self.corr_id = str(np.random.rand())

                with self.internal_lock:
                    self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.message_queue_names[self.to_worker_num],
                                               properties=pika.BasicProperties(reply_to=self.feedback_queue_names[self.to_worker_num], correlation_id=self.corr_id),
                                               body=task)

                # RoundRobin - advancing id of the worker so the next task will be sent to the next worker
                self.to_worker_num += 1
                if self.to_worker_num == num_of_workers_in_system+1:
                    self.to_worker_num = 1

        # # close connection
        # self.not_finished = "finished"

        # FOR DEBUG
        # # self.listen_workers_thread.join()
        # self.connection.close()


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

    def divide_job_into_tasks(self, curr_job_num):
        """
        Divide one matrix into sub-matrices, each sub-matrix is a task
        :param curr_job_num: job number
        :return: k - amount of tasks to complete job

        helper params:  n - job matrix is nxn
                        s = n/(number of columns in a sub matrix)
                        t = n/(number of rows in a sub matrix)
        """
        s = 2
        t = 2
        k = s*t # no redundancy
        n = 4

        task_num = 1
        matrix = self.all_jobs[curr_job_num]
        for y_idx in range(0, t):
            for x_idx in range(0, s):
                # create sub-matrix for task
                col_num_in_sub_mat = int(n / s)
                row_num_in_sub_mat = int(n / t)

                col_start = col_num_in_sub_mat * x_idx
                row_start = row_num_in_sub_mat * y_idx

                col_end = int(col_num_in_sub_mat * (x_idx + 1) - 1)
                row_end = int(row_num_in_sub_mat * (y_idx + 1) - 1)

                sub_mat = matrix[row_start:row_end][col_start:col_end]

                # write the task to a file in the nfs
                work_file_name = f'{self.path}/job{curr_job_num}_task{task_num}'
                with open(work_file_name, 'w') as f_work:
                    f_work.write(json.dumps(sub_mat))

                task_num += 1

        return k

    def data_for_fusion(self):
        """
        Create a file in nfs that contains data for fusion
        """
        fusion_file = "data_for_fusion"
        file_path = f'{self.path}/{fusion_file}'
        start_time = time.time()
        data = {"start_time": start_time, "num_of_workers_in_system": num_of_workers_in_system,
                "jobs_amount": len(self.all_jobs.keys())}
        print("[data_for_fusion]: data: " + str(data))  # FOR DEBUG

        with open(file_path, 'w') as f_data_for_fusion:
            f_data_for_fusion.write(json.dumps(data))


class worker_record(object):
    def __init__(self, worker_num):
        self.worker_num = worker_num
        self.average_work_time = -1
        self.busy = False

def main():
    producer = Producer()
    res = producer.send_task_wait_for_feedback()

    # after we sent all the jobs we wait for all the worker responses (in the thread)
    producer.listen_workers_thread.join()
    # we finished all the jobs - we can close the connection
    producer.connection.close()


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









