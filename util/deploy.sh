#!/bin/bash

docker stop -t 60 api
docker wait api
docker rm api
docker build -t hcdp_api .

docker run --restart on-failure --name=api -d -p 443:443 \
-v /mnt/lustre/annotated/HCDP:/data \
-v /home/hcdp/hcdp-api/logs:/logs \
hcdp_api