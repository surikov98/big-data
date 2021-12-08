#!/usr/bin/env bash

echo "Restarting api docker-compose ..."

# -f - force, -s - stop containers, -v remove anonymous volumes
docker-compose rm -fsv
docker-compose up -d --build