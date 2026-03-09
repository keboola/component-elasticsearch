FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_SYSTEM_PYTHON=1

WORKDIR /code/

# Install dependencies first for better layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --all-groups --no-install-project

# Copy source and supporting files
COPY /src /code/src/
COPY /tests /code/tests/
COPY /scripts /code/scripts/
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

CMD ["python", "-u", "/code/src/component.py"]
