# One image, three roles selected by the container command: api | load | rebuild (and send).
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY game_log_pipeline ./game_log_pipeline
RUN pip install ".[services]" && useradd --uid 10001 --no-create-home glp
USER 10001
EXPOSE 8080
ENTRYPOINT ["python", "-m", "game_log_pipeline"]
CMD ["api"]
