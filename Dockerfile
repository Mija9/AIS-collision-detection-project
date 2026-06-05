FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless bash procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY README.md REPORT.md ./
RUN mkdir -p data outputs

CMD ["python", "-m", "src.main"]
