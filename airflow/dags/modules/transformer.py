# import paho.mqtt.client as mqtt
# from cassandra.cluster import Cluster
#
# def on_message(client, userdata, msg):
#     data = json.loads(msg.payload)
#     temp_c = (data['temperature'] - 32) * 5.0/9.0
#     battery_percent = (data['battery_level'] / 5000.0) * 100
#
#     query = "INSERT INTO measurements (id, temperature, battery_level) VALUES (uuid(), %s, %s)"
#     databank_handler.execute(query, (temp_c, battery_percent))
#
# client = mqtt.Client()
# client.connect("mqtt", 1883, 60)
# client.on_message = on_message
# client.subscribe("iot/data")
# client.loop_forever()
#
import uuid
import json
import boto3
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

import logging

import socket
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NUM_RETRIES = 10

class Services:
    def __init__(self):
        self.databank_conn_handler = None
        self.databank_handler = None
        self.datalake_handler = None

    def ConnectToDatabank(self):
        """Connect to Cassandra (Databank)"""
        try:
            self.databank_conn_handler = Cluster(
                contact_points=["my_db"],
                port=9042,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            )
            self.databank_handler = self.databank_conn_handler.connect()

            # Create keyspace and table if not exists
            self.databank_handler.execute(
                """
			CREATE KEYSPACE IF NOT EXISTS iot
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
			"""
            )
            self.databank_handler.execute(
                """
			CREATE TABLE IF NOT EXISTS iot.measurements (
				id UUID PRIMARY KEY,
				temperature float,
				battery_level float
			)
			"""
            )
            return True
        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}")
            return False

    def ConnectToDatalake(self):
        """Connect to Minio (Datalake)"""
        try:
            self.datalake_handler = boto3.client(
                "s3",
                endpoint_url="http://172.20.0.2:9000",
                aws_access_key_id="minio",
                aws_secret_access_key="minio123",
            )
            return True
        except Exception as e:
            logger.error(f"Error connecting to Minio: {e}")
            return False

    def Close(self):
        self.databank_conn_handler.shutdown()

    def GetServices(self):
        return [self.databank_handler, self.datalake_handler]


class Transformer:
    def __init__(self):
        self.services = Services()

    def ProcessDataFromDatalake(self):
        is_connected = False
        logger.info("Connecting to Cassandra and Minio")
        for _ in range(NUM_RETRIES):
            if self.services.ConnectToDatabank() and self.services.ConnectToDatalake():
                is_connected = True
                break
            time.sleep(5)
        if not is_connected:
            logger.error("Could not connect to Cassandra or Minio")
            return

        [databank_handler, datalake_handler] = self.services.GetServices()
        response = datalake_handler.list_objects_v2(Bucket="test-data")
        logger.info(f"Files in the bucket: {response.get('Contents', [])}")

        for obj in response.get("Contents", []):
            key = obj["Key"]
            # Download the JSON file
            file_obj = datalake_handler.get_object(Bucket="test-data", Key=key)
            data = json.loads(file_obj["Body"].read().decode("utf-8"))

            logger.info(f"Processing file: {key}")
            logger.info(f"Data: {data}")

            # Transform the data
            temp_c, battery_percent = self.TransformData(data)
            logger.info(f"Transformed data: {temp_c}, {battery_percent}")

            # Store transformed data in Cassandra
            query = "INSERT INTO iot.measurements (id, temperature, battery_level) VALUES (%s, %s, %s)"
            id = uuid.uuid4()
            databank_handler.execute(query, (id, temp_c, battery_percent))

    def TransformData(self, data):
        temp_c = (data["temperature"] - 32) * 5.0 / 9.0
        battery_percent = (data["battery_level"] / 5000.0) * 100
        return temp_c, battery_percent


if __name__ == "__main__":
    transformer = Transformer()
    transformer.ProcessDataFromDatalake()
    transformer.services.Close()
