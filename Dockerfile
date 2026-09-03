FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=2.0.1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl build-essential default-libmysqlclient-dev pkg-config \
 && pip install --upgrade pip "poetry==$POETRY_VERSION" \
 && poetry config virtualenvs.create false \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN if [ -f poetry.lock ]; then \
      poetry install --no-interaction --no-ansi --only main; \
    else \
      poetry install --no-interaction --no-ansi --only main --no-root; \
    fi

COPY scheduler/ ./scheduler/

EXPOSE 9101

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://127.0.0.1:9101/health || exit 1

CMD ["python", "-m", "scheduler.main"]