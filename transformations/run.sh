#!/bin/bash

# if my-datalake or my-db container is not running, print an error message and exit
if [ ! "$(docker ps -q -f name=my-datalake)" ]; then
    echo "my-datalake container is not running. Please run it first."
    exit 1
fi

if [ ! "$(docker ps -q -f name=my-db)" ]; then
    echo "my-db container is not running. Please run it first."
    exit 1
fi

# Stop and remove the container
docker stop my-transformation
docker rm my-transformation

# Build and run the container
docker build -t my-transformation .
docker run -d --name my-transformation --link my-datalake:my-datalake --link my-db:my-db my-transformation
