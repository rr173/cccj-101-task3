# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    EA_DATA_DIR=/data \
    EA_ADDR=0.0.0.0:8080

WORKDIR /app
COPY eventarch ./eventarch

RUN useradd --system --uid 10001 --home /app eventarch \
    && mkdir -p /data \
    && chown -R eventarch:eventarch /data /app
USER eventarch

EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/v1/healthz',timeout=2)"

CMD ["python", "-m", "eventarch.server"]
