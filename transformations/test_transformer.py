import json
import boto3
from cassandra.cluster import Cluster
import uuid

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Cassandra connection
cluster = Cluster(["my-db"])
session = cluster.connect()

# Use the IP address of the Minio container, not localhost
# botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://localhost:9000/test-data?list-type=2&encoding-type=url"
# http://172.17.0.2:9001/browser/test-data/

s3 = boto3.client(
    "s3",
    endpoint_url="http://172.17.0.2:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123",
)


def transform_data(data):
    temp_c = (data["temperature"] - 32) * 5.0 / 9.0
    battery_percent = (data["battery_level"] / 5000.0) * 100
    return temp_c, battery_percent


def process_data_from_minio():
    # List all files in the bucket
    response = s3.list_objects_v2(Bucket="test-data")
    logger.info(f"Files in the bucket: {response.get('Contents', [])}")
    print(response.get("Contents", []))
    for obj in response.get("Contents", []):
        key = obj["Key"]
        # Download the JSON file
        file_obj = s3.get_object(Bucket="test-data", Key=key)
        data = json.loads(file_obj["Body"].read().decode("utf-8"))

        logger.info(f"Processing file: {key}")
        logger.info(f"Data: {data}")

        # Transform the data
        temp_c, battery_percent = transform_data(data)
        logger.info(f"Transformed data: {temp_c}, {battery_percent}")

        # Store transformed data in Cassandra
        query = "INSERT INTO iot.measurements (id, temperature, battery_level) VALUES (%s, %s, %s)"
        id = uuid.uuid4()
        session.execute(query, (id, temp_c, battery_percent))


if __name__ == "__main__":
    process_data_from_minio()
