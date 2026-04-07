FROM python:3.12-slim

LABEL maintainer="Glenn <schatzyyc.ca>"
LABEL description="CAISO ATL_PUB OASIS scraper — runs continuously, persists to Postgres"

WORKDIR /app

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scraper.py .

# credentials.json is mounted at runtime — never baked into the image
# See docker-compose.yml volume mount

CMD ["python", "-u", "scraper.py"]
