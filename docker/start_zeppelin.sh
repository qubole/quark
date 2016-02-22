#!/bin/bash

sudo docker build -t $1:$2 .

sudo docker run -i -t -p 8080:5050 $1:$2
