#!/bin/bash

# TODO: create containers following a pattern, so I can stop them all at once
source delete_containers.sh
IMAGE_IDS=$(docker images -q)

if [ -z "$IMAGE_IDS" ]; then
    echo "No images to remove"
    exit 0
fi

# Remove all images
echo "Removing images $IMAGE_IDS"
docker rmi -f $IMAGE_IDS

