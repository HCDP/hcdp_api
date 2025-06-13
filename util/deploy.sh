#!/bin/bash

docker compose --profile prod build
docker compose --profile prod up -d