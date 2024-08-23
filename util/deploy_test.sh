#!/bin/bash

docker stop apitest
docker wait apitest
docker rm apitest

cp -R ../api/certs/live certs
cp -R ../api/certs/archive certs

docker build -t hcdp_api_test .

docker run --name=apitest -d -p 8443:443 \
-v /mnt/lustre/annotated/HCDP:/data \
-v /home/hcdp/hcdp-api/logs:/logs \
hcdp_api_test