FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

#CMD ["python", "app.py"]

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
