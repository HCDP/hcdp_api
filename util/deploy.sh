#!/bin/bash

docker stop -t 60 email
docker wait email
docker rm email
docker build -t hcdp_email_api .

docker run --restart on-failure --name=email -d -p 443:443 \
-v /mnt/netapp/ikewai/annotated/HCDP:/data \
-v /home/hcdp/hcdp-api/logs:/logs \
hcdp_email_api