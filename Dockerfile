# Use official Python 3.9 slim image as base
FROM python:3.9-slim


WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY data/ ./data/

ENV PYTHONUNBUFFERED=1

CMD ["python", "src/main.py"]
