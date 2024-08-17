# Inspect the ip address of the docker container

```
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' my-datalake
```

# test mqtt broker

```
cd mqtt
docker build -t my-broker .
docker run -d --name my-broker -p 1883:1883 my-broker
```

test mqtt broker with mosquitto_sub

```
docker exec -it my-broker mosquitto_sub -h localhost -t test
```
in another terminal

```
docker exec -it my-broker mosquitto_pub -h localhost -t test -m "hello"
```

# test datalake (minio)

```
cd datalake
docker build -t my-datalake .
docker run -d --name my-datalake -p 9000:9000 -e "MINIO_ACCESS_KEY=minio" -e "MINIO_SECRET_KEY=minio123" my-datalake server /data --console-address ":9001"
```

open http://localhost:9000 in browser

```
access key: minio
secret key: minio123
```

to if localhost:9000 is not accessible, try to use the ip address of the docker container

```
$ docker logs my-datalake
API: http://172.17.0.4:9000  http://127.0.0.1:9000 
WebUI: http://172.17.0.4:38597 http://127.0.0.1:38597 
```

# test database (cassandra)

it may take a while to start the database

```
cd database
docker build -t my-db .
docker run -d --name my-db -p 9042:9042 my-db
```

test database (cassandra) with cqlsh as standalone

```
docker exec -it my-db cqlsh localhost
```

run the following commands in cqlsh

```
CREATE KEYSPACE iot WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE iot;
CREATE TABLE measurements (id UUID PRIMARY KEY, temperature float, battery_level float);
INSERT INTO measurements (id, temperature, battery_level) VALUES (uuid(), 25.0, 50.0);
SELECT * FROM measurements;
```

# test fastapi

test cassandra with fastapi
```
cd restapi
docker build -t api .
docker run -d --name api -p 8000:8000 --link my-db:my-db api
```

for debugging, run the following command
```
docker run -it --name api -p 8000:8000 --link my-db:my-db api bash


```sh
# Send data to the database
curl -X GET "http://localhost:8000/data/{id}" -H  "accept: application/json" -d '{"temperature": 25.0, "battery_level": 50.0}'
# Get data from the database
curl -X POST "http://localhost:8000/data/{id}" -H  "accept: application/json"
```

# test transformation (ELT)

First create dummy data in the datalake

```
python datalake/create_test_data.py
```

Testing transforming data from datalake to database using a python script within a docker container

```
cd transformation
docker build -t my-transformation .
docker run -d --name my-transformation --link my-datalake:my-datalake --link my-db:my-db my-transformation
```

verify it in the database

```
docker exec -it my-db cqlsh localhost
```

run the following commands in cqlsh

```
USE iot;
SELECT * FROM measurements;
```

# Test MQTT with DataLake

```
docker-compose -f docker-compose.yml.mqtt_test up --build
```

from another terminal

```
cd mqtt/
python mqtt_publisher_test.py
```

# Test with airflow

```
docker compose up
```

# stop all 

```
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
```

# delete all images

```
docker rmi $(docker images -q)
```

## TODO
[ ] transformation dags is running the whole time
