# Use official Python 3.9 slim image as base
FROM python:3.9-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and data directories
COPY src/ ./src/
COPY data/ ./data/

# Set environment variable to avoid Python buffering issues
ENV PYTHONUNBUFFERED=1

# Default command to run the pipeline
CMD ["python", "src/main.py"]