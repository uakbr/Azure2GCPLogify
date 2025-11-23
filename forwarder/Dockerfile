FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
# We do NOT copy config.example.yaml to config.yaml to avoid accidental usage of defaults.
# Users must mount config.yaml or we can copy it if it exists (but better to fail if missing).
COPY config.example.yaml config.example.yaml

# Set python path to include src
ENV PYTHONPATH=/app/src

CMD ["python", "src/main.py"]
