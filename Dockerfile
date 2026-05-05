# syntax=docker/dockerfile:1
# KNGTOP — BTC/ETH/XRP PM misprice (5m + 15m), $1 v2 market buys
FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Persist pip downloads across rebuilds when BuildKit is enabled.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY kngtop/ ./kngtop/

RUN mkdir -p /app/logs

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

ENV POLY_DRY_RUN=true

CMD ["python", "-m", "kngtop"]
