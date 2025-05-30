#!/bin/bash

docker compose --profile prod build
docker compose --profile dev up -d