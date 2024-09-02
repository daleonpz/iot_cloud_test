import uuid
import json
import boto3
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

import tqdm

import logging

import socket
import time

import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NUM_RETRIES = int(os.getenv("NUM_RETRIES", 3))

MINIO_HOST = os.getenv("MINIO_HOST", "docker_datalake")
MINIO_PORT = os.getenv("MINIO_PORT", 9000)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "test_data")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "my_db")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", 9042)


class Services:
    def __init__(self):
        self.databank_conn_handler = None
        self.databank_handler = None
        self.datalake_handler = None

    def ConnectToDatabank(self):
        """Connect to Cassandra (Databank)"""
        try:
            self.databank_conn_handler = Cluster(
                contact_points=[CASSANDRA_HOST],
                port=CASSANDRA_PORT,
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
                endpoint_url=f"http://{MINIO_HOST}:{MINIO_PORT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
            )
            return True
        except Exception as e:
            logger.error(f"Error connecting to Minio: {e}")
            return False

    def Close(self):
        logger.info("Closing connections")

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

        # Check if the bucket exists
        try:
            datalake_handler.head_bucket(Bucket=MINIO_BUCKET)
        except Exception as e:
            logger.error(f"Bucket does not exist: {e}")
            return

        response = datalake_handler.list_objects_v2(Bucket=MINIO_BUCKET)
        logger.debug(f"Files in the bucket: {response.get('Contents', [])}")

        # Process each file in the bucket
        for obj in tqdm.tqdm(response.get("Contents", [])):
            key = obj["Key"]
            # Download the JSON file
            file_obj = datalake_handler.get_object(Bucket=MINIO_BUCKET, Key=key)
            data = json.loads(file_obj["Body"].read().decode("utf-8"))

            logger.debug(f"Processing file: {key}")
            logger.debug(f"Data: {data}")

            # Transform the data
            temp_c, battery_percent = self.TransformData(data)
            logger.debug(f"Transformed data: {temp_c}, {battery_percent}")

            # Store transformed data in Cassandra
            query = "INSERT INTO iot.measurements (id, temperature, battery_level) VALUES (%s, %s, %s)"
            id = uuid.uuid4()
            databank_handler.execute(query, (id, temp_c, battery_percent))

    def TransformData(self, data):
        temp_c = (data["temperature"] - 32) * 5.0 / 9.0
        battery_percent = (data["battery_level"] / 5000.0) * 100
        return temp_c, battery_percent


import random


class TestPreparation:
    """Prepare test data for the datalake"""

    def __init__(self):
        self.services = Services()

    def PrepareDataForDatalake(self):
        is_connected = False
        logger.info("Connecting to Minio")
        for _ in range(NUM_RETRIES):
            if self.services.ConnectToDatalake():
                is_connected = True
                break
            time.sleep(5)
        if not is_connected:
            logger.error("Could not connect to Minio")
            return

        [_, datalake_handler] = self.services.GetServices()

        # Check if the bucket exists, if not create it
        try:
            datalake_handler.head_bucket(Bucket=MINIO_BUCKET)
        except Exception as e:
            logger.error(f"Bucket does not exist: {e}")
            datalake_handler.create_bucket(Bucket=MINIO_BUCKET)

        # Prepare test data
        test_data = [
            {
                "temperature": random.randint(50, 100),
                "battery_level": random.randint(2000, 5000),
            },
            {
                "temperature": random.randint(50, 100),
                "battery_level": random.randint(2000, 5000),
            },
            {
                "temperature": random.randint(50, 100),
                "battery_level": random.randint(2000, 5000),
            },
        ]

        for i, data in enumerate(test_data):
            datalake_handler.put_object(
                Bucket=MINIO_BUCKET, Key=f"data_{i}.json", Body=json.dumps(data)
            )

        self.services.Close()


if __name__ == "__main__":
    # Prepare test data
    test_preparation = TestPreparation()
    test_preparation.PrepareDataForDatalake()

    # Start ETL process
    transformer = Transformer()
    transformer.ProcessDataFromDatalake()
    transformer.services.Close()
