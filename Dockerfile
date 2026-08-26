FROM python:3.12-slim

WORKDIR /app

# git is required at runtime -- the Prefect worker fetches this flow's code
# fresh from GitRepository on every run (see deploy.py), inside this image.
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flow/ ./flow/
COPY routing.py .
