#!/bin/bash

((num_of_workers=3))

# clean all main-worker queues
for (( i=1; i<=num_of_workers; i++ ))
  do
  command="sudo rabbitmqctl purge_queue w${i}_main -p proj_host"
  $command
  done

# clean all worker-fusion queues
for (( i=1; i<=num_of_workers; i++ ))
  do
  command="sudo rabbitmqctl purge_queue w${i}_fusion -p proj_host"
  $command
  done