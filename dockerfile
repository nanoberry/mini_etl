FROM ghcr.io/astral-sh/uv:debian-slim
WORKDIR /app
COPY . .
RUN uv sync --locked
ENTRYPOINT ["uv", "run", "top_10.py"]