#!/bin/bash

export API_VERSION="$(git rev-parse --short HEAD)-$(date +%Y%m%d%H%M)"

docker compose --profile prod build
docker compose --profile prod up -d