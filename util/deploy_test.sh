#!/bin/bash

docker stop emailtest
docker wait emailtest
docker rm emailtest

cp -R ../api/certs/live certs
cp -R ../api/certs/archive certs

docker build -t hcdp_email_api_test .

docker run --name=emailtest -d -p 8443:443 \
-v /mnt/netapp/ikewai/annotated/HCDP:/data \
-v /home/hcdp/hcdp-api/logs:/logs \
hcdp_email_api_test