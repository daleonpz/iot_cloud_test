import paho.mqtt.client as mqtt
import boto3
import json
import time
import os
from io import BytesIO

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self, endpoint, access_id, secret_key, bucket_name):
        self.endpoint = endpoint
        self.access_id = access_id
        self.secret_key = secret_key
        self.bucket_name = bucket_name

    def connect(self):
        try:
            self.client = boto3.client(
                's3',
                endpoint_url=f'http://{self.endpoint}',
                aws_access_key_id=self.access_id,
                aws_secret_access_key=self.secret_key
            )
            logger.info("Connected to MinIO")
            return True
        except Exception as e:
            logger.error(f"Error connecting to MinIO: {e}")
            return False

    def create_bucket(self):
        try:
            self.client.create_bucket(Bucket=self.bucket_name)
            logger.info(f"Created bucket {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to create bucket {self.bucket_name}: {e}")

    def save_json(self, file_name, data):
        try:
            file_data = BytesIO(json.dumps(data).encode('utf-8'))
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=file_data,
                ContentType='application/json'
            )
            logger.info(f"Successfully saved {file_name} to MinIO")
        except Exception as e:
            logger.error(f"Failed to save {file_name} to MinIO: {e}")

class MQTTMinIOConnector:
    def __init__(self, mqtt_broker, mqtt_port, mqtt_topic, minio_client):
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic
        self.minio_client = minio_client

    def Connect(self):
        try:
            self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
            logger.info(f"Connected to MQTT broker at {self.mqtt_broker}:{self.mqtt_port}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to MQTT broker: {e}")
            return False

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc != 0:
            logger.error(f"Failed to connect to MQTT broker with return code {rc}")
            return False

        logger.info(f"Subscribing to topic: {self.mqtt_topic}")
        client.subscribe(self.mqtt_topic)
        return True

    def on_message(self, client, userdata, msg):
        logger.info(f"Received message on {msg.topic}: {msg.payload.decode()}")
        # Parse message payload (assuming it's JSON)
        payload = json.loads(msg.payload.decode())
        
        # Generate file name based on timestamp + topic
        file_name = f"{int(time.time())}_{msg.topic.replace('/', '_')}.json"
        
        # Save the data to MinIO
        self.minio_client.save_json(file_name, payload)

    def loop(self):
        self.mqtt_client.loop_forever()

if __name__ == '__main__':
    MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
    MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
    MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'test/dummy_topic')
    MQTT_NUM_RETRIES = int(os.getenv('NUM_RETRIES', 10))

    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost')
    MINIO_PORT = os.getenv('MINIO_PORT', '9000')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')
    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'test-data')
    MINIO_NUM_RETRIES = int(os.getenv('NUM_RETRIES', 3))

    # Initialize MinIO client
    minio_client = MinIOClient(
        endpoint=MINIO_ENDPOINT + ':' + MINIO_PORT,
        access_id=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET_NAME
        )

    minio_is_connected = False
    for _ in range(MINIO_NUM_RETRIES):
        if minio_client.connect():
            minio_is_connected = True
            break
        time.sleep(5)

    if not minio_is_connected:
        logger.error("Could not connect to MinIO")
        exit(1)

    minio_client.create_bucket()
    # Initialize and start MQTT to MinIO connector
    connector = MQTTMinIOConnector(
        mqtt_broker=MQTT_BROKER,
        mqtt_port=MQTT_PORT,
        mqtt_topic=MQTT_TOPIC,
        minio_client=minio_client
    )

    mqtt_is_connected = False
    for _ in range(MQTT_NUM_RETRIES):
        if connector.Connect():
            mqtt_is_connected = True
            break
        time.sleep(5)

    if not mqtt_is_connected:
        logger.error("Could not connect to MQTT broker")
        exit(1)

    connector.loop()

