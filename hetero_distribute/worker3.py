import pika
import sys
import os
import time
import json
import uuid
import numpy as np
import ast
import math

# Adding a file from the nfs that contains all the ips, usernames and passwords
sys.path.append("/var/nfs/general") # from computer
sys.path.append("/nfs/general") # from servers

import cred

local_nfs_path = "/var/nfs/general"
server_nfs_path = "/nfs/general"

class Worker(object):
    def __init__(self):
        ###### ------------ fusion variables init ------------ ######
        self.worker_num = 3
        self.forbidden_jobs = []
        self.forbidden_val = 0 # under this job ID all jobs are forbidden
        self.tasks_counter = 0
        self.average_work_time = 0

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
        self.worker_queue_name = "w" + str(self.worker_num) + "_main"
        self.channel.queue_declare(queue=self.worker_queue_name)

        # creating feedback queue to the main node
        self.feedback_from_worker_queue_name = "fb_w" + str(self.worker_num) + "_main"
        self.channel.queue_declare(queue=self.feedback_from_worker_queue_name)

        # connect exchange and queue
        self.channel.queue_bind(exchange=self.main_exchange_name, queue=self.worker_queue_name)
        self.channel.queue_bind(exchange=self.main_exchange_name, queue=self.feedback_from_worker_queue_name)

        # --------------------- parameters for fusion node --------------------- #
        # establish a connection with RabbitMQ server
        # using our vhost named 'proj_host' in IP <cred.pc_ip> and port 5672
        self.connection_fusion = pika.BlockingConnection(pika.ConnectionParameters(cred.pc_ip, 5672, cred.rbt_vhost, self.credentials))
        self.channel_fusion = self.connection_fusion.channel()

        # creating an exchange with fusion node
        self.fusion_exchange_name = 'fusion_exchange'
        self.channel_fusion.exchange_declare(exchange=self.fusion_exchange_name, exchange_type='direct')

        # create fusion queues
        self.fusion_queue_name = "w" + str(self.worker_num) + "_fusion"
        self.fusion_queue_declare = self.channel_fusion.queue_declare(queue=self.fusion_queue_name)
        self.fusion_feedback_queue_name = str(self.fusion_queue_declare.method.queue)

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
        # print("[fusion_response]: self.corr_id: " + str(self.corr_id)) # FOR DEBUG
        # print("[fusion_response]: props.correlation_id: " + str(props.correlation_id)) # FOR DEBUG
        # print("[fusion_response]: body: " + str(body)) # FOR DEBUG

        if "[v] from fusion" in str(body):
            # print("[fusion_response]: inside if")  # FOR DEBUG
            self.response_from_fusion = ast.literal_eval(json.dumps(body.decode()))
            self.response_from_fusion  = ast.literal_eval(self.response_from_fusion ) # we need DOUBLE unpacking because we have double casting to str

            # update forbidden_val
            self.update_forbidden_jobs()


    def send_to_fusion_and_wait_for_feedback(self):
        #############################################
        # send the task result to fusion node and wait for response
        #############################################

        # init
        self.response_from_fusion = None

         # send to fusion
        self.corr_id = str(np.random.rand())
        self.channel_fusion.basic_publish(exchange=self.fusion_exchange_name, routing_key=self.fusion_queue_name,
                                             properties=pika.BasicProperties(reply_to=self.fusion_feedback_queue_name,
                                                                             correlation_id=self.corr_id), body=str(self.result))
        print(f' [x] Sent task to fusion')

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
        task = ast.literal_eval(task) # we need DOUBLE unpacking because we have double casting to str

        # read task
        header, task_content, task_result_file_name = self.unpack_task(task)
        print("[work]: working on job: " + str(header["job_number"]) + " task: " + str(header["task_number"])) # FOR DEBUG

        # making sure we got a task in a valid job (a job that is still in progress)
        if (header["job_number"] in self.forbidden_jobs) or (header["job_number"] <= self.forbidden_val):
            print(f' [x] The job {header["job_number"]} is already done\n\n')

            # sending feedback to main
            # TODO: we need to add if we are free and how long we worked (if we have worked)
            response_to_main = ' [v] from worker: worker1 is done'

            ch.basic_publish(exchange=self.main_exchange_name, routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                             body=str(response_to_main))
        else:
            self.tasks_counter += 1
            # compute result
            task_content = self.multiply_polynomes(task_content)
            self.result = {"Header": header, "result": task_content}

            # write answer
            out_file = open(task_result_file_name, 'w')
            out_file.write(json.dumps(self.result))
            out_file.close()
            print(' [x] Saved json file')

            self.result["result"] = task_result_file_name

            # sending feedback to main
            response_to_main = {"response": f' [v] from worker: worker {self.worker_num} is done: job {header["job_number"]} , task {header["task_number"]}',
                                "worker_num": self.worker_num ,"average_time": self.average_work_time}

            ch.basic_publish(exchange=self.main_exchange_name, routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                             body=str(response_to_main))

            # sending task result to fusion node
            self.send_to_fusion_and_wait_for_feedback()

        print(' [x] Done\n\n')
        return

    def unpack_task(self, task):
        """

        """
        # print("[unpack_task] The task received from main: " + str(task)) # FOR DEBUG
        header = task["Header"]
        header["worker_num"] = self.worker_num

        # load the tasks content
        task_file_name = task["task"]
        with open(task_file_name, 'r') as f:
            task_content = str(json.load(f))

        return header, task_content, task_file_name

    def multiply_polynomes(self, task_content):
        """

        """
        # generate random sleep (=work) time from normal distribute
        mu, sigma = 6, 0.5 # mean and standard deviation
        sleep_time = float(np.random.normal(mu, sigma, 1))
        print("[multiply_polynomes] generated sleep time: " + str(sleep_time)) # FOR DEBUG

        # Do the work (sleep) and measure the work time
        start_time = time.time()
        time.sleep(sleep_time) # TODO change to simulation results
        end_time = time.time()
        work_time = end_time - start_time

        # Update mean_work_time
        self.average_work_time = ((self.tasks_counter - 1) * self.average_work_time + work_time) / self.tasks_counter
        return task_content

    def update_forbidden_jobs(self):
        """
        Update self.forbidden_val (value that represents the border which all job number equal or under it are forbidden)
        and self.forbidden_jobs (list contains forbidden jobs numbers, all above self.forbidden_val)
        :return:
        """
        # If new forbidden job is above self.forbidden_val, enter it to self.forbidden_jobs. Else it included already in self.forbidden_val
        for job_num in self.response_from_fusion["forbidden_jobs"]:
            if job_num > self.forbidden_val:
                self.forbidden_jobs += [job_num]

        self.forbidden_jobs.sort()

        # Update self.forbidden_val to max consecutive number
        # if the list of forbidden jobs is not empty AND forbidden vals needs to be updated
        while len(self.forbidden_jobs) > 0 \
                and self.forbidden_jobs[self.binary_search_upper(self.forbidden_jobs, self.forbidden_val)] == self.forbidden_val + 1:
            self.forbidden_val += 1
            self.forbidden_jobs = self.forbidden_jobs[self.binary_search_upper(self.forbidden_jobs, self.forbidden_val)+1:]

    def binary_search_upper(self, arr, x):
        """
        Iterative Binary Search Function
        :param arr: array to search in
        :param x: value to search
        :return: index of x in given array arr if present, else returns the closest upper value
        """
        low = 0
        high = len(arr) - 1
        mid = 0
        while low <= high:
            mid = math.floor((high + low) / 2)
            # If x is greater, ignore left half
            if arr[mid] < x:
                low = mid + 1

            # If x is smaller, ignore right half
            elif arr[mid] > x:
                high = mid - 1

            # means x is present at mid
            else:
                return mid

        # If we reach here, then the element was not present
        # we will return the closest higher value
        return min(low,len(arr)-1)

    def binary_search_lower(self, arr, x):
        """
        Iterative Binary Search Function
        :param arr: array to search in
        :param x: value to search
        :return: index of x in given array arr if present, else returns the closest lower value
        """
        low = 0
        high = len(arr) - 1
        mid = 0
        while low <= high:
            mid = math.floor((high + low) / 2)
            # If x is greater, ignore left half
            if arr[mid] < x:
                low = mid + 1

            # If x is smaller, ignore right half
            elif arr[mid] > x:
                high = mid - 1

            # means x is present at mid
            else:
                return mid

        # If we reach here, then the element was not present
        # we will return the closest higher value
        return max(high,0)

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