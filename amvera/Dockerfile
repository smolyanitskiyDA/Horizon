FROM python:3.12-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY profiles ./profiles
COPY data/config.example.json ./data/config.example.json
RUN uv sync --frozen --no-dev
COPY amvera/worker.py amvera/digest.py amvera/feeds.json ./amvera/
ENV PYTHONUNBUFFERED=1 HORIZON_CHATGPT_AUTH_DIR=/data/chatgpt HOME=/home/horizon
RUN useradd --create-home --uid 10001 horizon
ENTRYPOINT ["/app/.venv/bin/python", "-m", "amvera.worker"]
