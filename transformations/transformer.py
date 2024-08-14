# import paho.mqtt.client as mqtt
# from cassandra.cluster import Cluster
# 
# def on_message(client, userdata, msg):
#     data = json.loads(msg.payload)
#     temp_c = (data['temperature'] - 32) * 5.0/9.0
#     battery_percent = (data['battery_level'] / 5000.0) * 100
# 
#     query = "INSERT INTO measurements (id, temperature, battery_level) VALUES (uuid(), %s, %s)"
#     session.execute(query, (temp_c, battery_percent))
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
		self.cluster = None
		self.session = None
		self.s3 = None

	def connect_to_cassandra(self):
		try:
# 			self.cluster = Cluster(['my_db'])
			self.cluster = Cluster(contact_points=['my_db'], 
						  port=9042,
						  load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
						  )
			self.session = self.cluster.connect()
			self.session.execute("""
			CREATE KEYSPACE IF NOT EXISTS iot
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
			""")
			self.session.execute("""
			CREATE TABLE IF NOT EXISTS iot.measurements (
				id UUID PRIMARY KEY,
				temperature float,
				battery_level float
			)
			""")
			return True
		except Exception as e:
			logger.error(f"Error connecting to Cassandra: {e}")
			return False


	def connect_to_minio(self):
		try:
			self.s3 = boto3.client('s3', endpoint_url='http://172.20.0.2:9000',
				 aws_access_key_id='minio',
							 aws_secret_access_key='minio123')
			return True
		except Exception as e:
			logger.error(f"Error connecting to Minio: {e}")
			return False

	def close(self):
		self.cluster.shutdown()

	def get_services(self):
		return [self.session, self.s3]

def transform_data(data):
	temp_c = (data['temperature'] - 32) * 5.0 / 9.0
	battery_percent = (data['battery_level'] / 5000.0) * 100
	return temp_c, battery_percent

def process_data_from_minio():
	# List all files in the bucket
	services = Services()
	is_connected = False
	for _ in range(NUM_RETRIES):
		if services.connect_to_cassandra() and services.connect_to_minio():
			is_connected = True
			break
		time.sleep(5)
	if not is_connected:
		logger.error("Could not connect to Cassandra or Minio")
		return

	[session, s3] = services.get_services()

	response = s3.list_objects_v2(Bucket='test-data')
	logger.info(f"Files in the bucket: {response.get('Contents', [])}")
	print(response.get('Contents', []))
	for obj in response.get('Contents', []):
		key = obj['Key']
		# Download the JSON file
		file_obj = s3.get_object(Bucket='test-data', Key=key)
		data = json.loads(file_obj['Body'].read().decode('utf-8'))

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

