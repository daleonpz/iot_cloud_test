#!/bin/bash

# Stop and remove the container, if it exists
docker stop my-broker
docker rm my-broker

# Build and run the container
docker build -t my-broker .
docker run -d --name my-broker -p 1883:1883 my-broker
