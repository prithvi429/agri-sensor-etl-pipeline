# Dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY src/ src/
COPY tests/ tests/
COPY data/ data/
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "src/main.py"]
