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

#####################
# Minio Test
#####################

# from fastapi import FastAPI
# from fastapi.responses import StreamingResponse
# import boto3
# import io
# 
# app = FastAPI()
# 
# # s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
# s3 = boto3.client('s3', endpoint_url='http://172.17.0.3:9000',
#                   aws_access_key_id='minio',
#                   aws_secret_access_key='minio123')
# 
# @app.get("/image/{uuid}")
# async def get_image(uuid: str):
#     obj = s3.get_object(Bucket='images', Key=uuid)
#     return StreamingResponse(io.BytesIO(obj['Body'].read()), media_type="image/jpeg")
# 
# @app.get("/test")
# async def test():
#     return {"message": "API is working!"}
# 
#####################
# Cassandra Test
#####################
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import uuid

app = FastAPI()

# Connect to Cassandra
# cluster = Cluster(['cassandra'])
cluster = Cluster(['my-db'])
session = cluster.connect()

# Create keyspace and table if they don't exist
session.execute("""
CREATE KEYSPACE IF NOT EXISTS iot
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")

session.execute("""
CREATE TABLE IF NOT EXISTS iot.measurements (
    id UUID PRIMARY KEY,
    temperature float,
    battery_level float
)
""")

class Measurement(BaseModel):
    temperature: float
    battery_level: float

@app.post("/data/")
# async def insert_data(temperature: float, battery_level: float):
async def insert_data(measurement: Measurement):
	try:
		query = SimpleStatement("""
		INSERT INTO iot.measurements (id, temperature, battery_level)
		VALUES (%s, %s, %s)
		""")
		id = uuid.uuid4() 
		session.execute(query, (id, measurement.temperature, measurement.battery_level))
		return {"id": id, "temperature": measurement.temperature, "battery_level": measurement.battery_level}
	except Exception as e:
		return {"error": str(e)}

@app.get("/data/{data_id}")
async def get_data(data_id: str):
    query = SimpleStatement("SELECT * FROM iot.measurements WHERE id=%s")
    rows = session.execute(query, (uuid.UUID(data_id),))
    row = rows.one()
    if row:
        return {"id": row.id, "temperature": row.temperature, "battery_level": row.battery_level}
    else:
        raise HTTPException(status_code=404, detail="Data not found")

@app.put("/data/{data_id}")
# async def update_data(data_id: str, temperature: float, battery_level: float):
async def update_data(data_id: str, measurement: Measurement):
    query = SimpleStatement("""
    UPDATE iot.measurements
    SET temperature=%s, battery_level=%s
    WHERE id=%s
    """)
    session.execute(query, (measurement.temperature, measurement.battery_level, uuid.UUID(data_id)))
    return {"id": data_id, "temperature": measurement.temperature, "battery_level": measurement.battery_level}

#     session.execute(query, (temperature, battery_level, uuid.UUID(data_id)))
#     return {"id": data_id, "temperature": temperature, "battery_level": battery_level}

@app.delete("/data/{data_id}")
async def delete_data(data_id: str):
    query = SimpleStatement("DELETE FROM iot.measurements WHERE id=%s")
    session.execute(query, (uuid.UUID(data_id),))
    return {"message": "Data deleted successfully"}

