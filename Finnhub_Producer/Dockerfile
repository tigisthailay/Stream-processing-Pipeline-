# Dockerfile for Finnhub Producer
FROM python:3.9-slim

WORKDIR /app

# Install required packages
COPY requirements.txt .

# # Copy the producer and utils script
COPY Finnhub_Producer.py .
COPY utils.py .

# Copy all files in the directory into the container
# COPY . .

RUN pip install --no-cache-dir -r requirements.txt


# Set entrypoint
CMD ["python", "Finnhub_Producer.py"]
