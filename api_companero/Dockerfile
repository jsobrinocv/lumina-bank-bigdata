# Imagen de la API de Lumina Bank para Cloud Run
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PORT=8080
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Cloud Run indica el puerto en la variable PORT
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1
