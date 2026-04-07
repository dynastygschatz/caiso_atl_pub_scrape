FROM python:3.12-slim

LABEL maintainer="Glenn <schatzyyc.ca>"
LABEL description="CAISO ATL_PUB OASIS scraper + queue worker — shared image, two services"

WORKDIR /app

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Both services share this image — command is overridden per-service in compose
COPY scraper.py .
COPY queue_worker.py .

# credentials.json is mounted at runtime — never baked into the image
# See docker-compose.yml volume mount

# Default: fetcher. Queue worker overrides this via docker-compose command:
CMD ["python", "-u", "scraper.py"]
