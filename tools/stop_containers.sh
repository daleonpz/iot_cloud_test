#!/bin/bash

# TODO: create containers following a pattern, so I can stop them all at once
CONTAINERS=$(docker ps -a -q)
IMAGE_IDS=$(docker images -q)

if [ -z "$CONTAINERS" ]; then
    echo "No containers to stop"
    exit 0
fi

# Stop all containers
docker stop "$CONTAINERS"
docker rm "$CONTAINERS"

if [ -z "$IMAGE_IDS" ]; then
    echo "No images to remove"
    exit 0
fi

# Remove all images
docker rmi -f "$IMAGE_IDS"

