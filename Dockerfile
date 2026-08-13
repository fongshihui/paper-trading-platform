FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY common ./common
COPY dashboard ./dashboard
COPY producers ./producers
COPY simulator ./simulator
COPY tests ./tests

CMD ["python", "-m", "producers.mock_prices_producer"]
