#!/bin/bash

# Check if database container is running, if not, print error message and exit

if [ ! "$(docker ps -q -f name=my-db)" ]; then
    echo "Database container is not running. Please start the database container first."
    exit 1
fi

# Stop and remove the api container if it is already running
docker stop api
docker rm api

# Build and run the api container
docker build -t api .
docker run -d --name api -p 8000:8000 --link my-db:my-db api
