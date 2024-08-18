import boto3
import json
import logging
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
                  aws_access_key_id='minio',
                  aws_secret_access_key='minio123')

test_data = [
        {"temperature": random.randint(50, 100), "battery_level": random.randint(2000, 5000)}, 
        {"temperature": random.randint(50, 100), "battery_level": random.randint(2000, 5000)},
        {"temperature": random.randint(50, 100), "battery_level": random.randint(2000, 5000)},
        ]

# test connection with MinIO
response = s3.list_buckets()

# if test-data bucket does not exist, create it
if 'test-data' not in [bucket['Name'] for bucket in response['Buckets']]:
    logger.info(f"Creating bucket: test-data")
    s3.create_bucket(Bucket='test-data')

logger.info(f"List of buckets: {response['Buckets']}")

for i, data in enumerate(test_data):
    s3.put_object(Bucket='test-data', Key=f'data_{i}.json', Body=json.dumps(data))

# verify the object was created
response = s3.list_objects_v2(Bucket='test-data')
logger.info(f"List of objects: {response['Contents']}")



