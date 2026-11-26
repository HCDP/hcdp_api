#!/bin/bash

export API_VERSION="$(git rev-parse --short HEAD)-$(date +%Y%m%d%H%M)-dev"

docker compose --profile dev build
docker stop api-dev
docker rm api-dev
docker compose --profile dev up -d