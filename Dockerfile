FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

WORKDIR /code/

# Install dependencies first for better layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --all-groups --no-install-project

# Copy source and supporting files
COPY src src/
COPY tests tests/
COPY scripts scripts/
COPY flake8.cfg .
COPY deploy.sh .

CMD ["python", "-u", "src/component.py"]
