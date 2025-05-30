#!/bin/bash

docker compose --profile dev build
docker compose --profile dev up -d