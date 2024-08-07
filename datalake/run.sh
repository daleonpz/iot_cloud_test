#!/bin/bash

# stop and delete the container, if it exists
docker stop my-datalake
docker rm my-datalake

# build the image and run the container
docker build -t my-datalake .
docker run -d --name my-datalake -p 9000:9000 -e "MINIO_ACCESS_KEY=minio" -e "MINIO_SECRET_KEY=minio123" my-datalake server /data --console-address ":9001"

