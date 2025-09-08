FROM ghcr.io/astral-sh/uv:debian-slim
WORKDIR /app
COPY . .
RUN uv sync --locked
ENTRYPOINT ["uv", "run", "src/top_10.py"]