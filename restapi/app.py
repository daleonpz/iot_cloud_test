# from fastapi import FastAPI
# import boto3
# from cassandra.cluster import Cluster
# 
# app = FastAPI()
# 
# s3 = boto3.client('s3', endpoint_url='http://minio:9000',
#                   aws_access_key_id='minio',
#                   aws_secret_access_key='minio123')
# 
# cluster = Cluster(['cassandra'])
# session = cluster.connect('iot')
# 
# @app.get("/image/{uuid}")
# async def get_image(uuid: str):
#     obj = s3.get_object(Bucket='images', Key=uuid)
#     return StreamingResponse(io.BytesIO(obj['Body'].read()), media_type="image/jpeg")
# 
# @app.get("/data")
# async def get_data():
#     rows = session.execute("SELECT * FROM measurements")
#     return rows
# 

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import boto3
import io

app = FastAPI()

# s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
s3 = boto3.client('s3', endpoint_url='http://172.17.0.3:9000',
                  aws_access_key_id='minio',
                  aws_secret_access_key='minio123')

@app.get("/image/{uuid}")
async def get_image(uuid: str):
    obj = s3.get_object(Bucket='images', Key=uuid)
    return StreamingResponse(io.BytesIO(obj['Body'].read()), media_type="image/jpeg")

@app.get("/test")
async def test():
    return {"message": "API is working!"}

