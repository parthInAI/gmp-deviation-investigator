# Python tools image: replayer, openFDA producer, registry CLI, data fetcher, gate check.
# Dependencies are installed from pyproject.toml alone, so editing code never
# re-runs pip. docker-compose bind-mounts the source (PYTHONPATH=/app), so code
# edits need no rebuild at all; rebuild only when dependencies change.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONPATH=/app
WORKDIR /app

COPY pyproject.toml ./
RUN mkdir -p streaming lakehouse \
 && touch README.md streaming/__init__.py lakehouse/__init__.py \
 && pip install ".[lakehouse]" \
 && pip uninstall -y gmp-deviation-investigator \
 && rm -rf streaming lakehouse README.md

COPY streaming ./streaming
COPY lakehouse ./lakehouse

CMD ["python", "-m", "streaming.replayer", "--help"]
