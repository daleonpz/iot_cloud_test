FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

#CMD ["python", "transformer.py"]
CMD ["python", "test_transformer.py"]

