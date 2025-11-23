FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY config.example.yaml config.yaml

# Set python path to include src
ENV PYTHONPATH=/app/src

CMD ["python", "src/main.py"]
