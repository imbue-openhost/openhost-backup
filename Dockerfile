FROM python:3.12-alpine

RUN apk add --no-cache rclone

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml .
RUN uv sync --no-dev --no-install-project

COPY app.py .
COPY templates/ templates/

EXPOSE 8080

CMD ["uv", "run", "hypercorn", "-b", "0.0.0.0:8080", "app:app"]
