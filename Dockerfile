FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app:$PYTHONPATH
ENV DAGSTER_HOME=/opt/dagster/home

RUN mkdir -p /opt/dagster/home

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
