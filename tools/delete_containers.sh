#!/bin/bash

# TODO: create containers following a pattern, so I can stop them all at once
CONTAINERS=$(docker ps -a -q)

if [ -z "$CONTAINERS" ]; then
    echo "No containers to stop"
else
    echo "Containers to stop: $CONTAINERS"
    docker stop $CONTAINERS
    docker rm $CONTAINERS
fi
