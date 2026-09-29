#!/bin/bash

docker stop api-dev
docker rm api-dev
docker compose --profile dev build
docker compose --profile dev up -d