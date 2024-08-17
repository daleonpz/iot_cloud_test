FROM python:3.9-slim

WORKDIR /app

COPY . /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "mqtt_to_minio.py"]
