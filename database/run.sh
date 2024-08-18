#!/bin/bash

# Stop and remove the container, if it exists
docker stop my-db
docker rm my-db

# Build and run the container
docker build -t my-db .
docker run -d --name my-db -p 9042:9042 my-db
