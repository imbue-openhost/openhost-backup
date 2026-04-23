FROM python:3.12-alpine

RUN apk add --no-cache restic

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync

COPY app.py migration.py operations.py ./
COPY templates/ templates/

EXPOSE 8080

CMD ["sh", "-c", "echo 'Starting hypercorn...' && uv run hypercorn -b 0.0.0.0:8080 app:app"]
