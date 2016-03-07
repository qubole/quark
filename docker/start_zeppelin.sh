#!/bin/bash

sudo docker build -t $1:$2 .

sudo docker run -d --name=$3 -p 0.0.0.0:8080:8080 $1:$2 tail -f /dev/null

sudo docker exec -it $3 /home/ubuntu/src/incubator-zeppelin/bin/zeppelin-daemon.sh start


