FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY telemetry_consumer.py .
COPY ../anomaly_detector ./anomaly_detector

CMD ["python", "telemetry_consumer.py"]
