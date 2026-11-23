# syntax=docker/dockerfile:1
# One image runs every pipeline service: the stream initializer, the replay producer, both consumer groups, the
# watchers, the dashboard and the orchestrator of the batch flows (docker-compose.yml gives each its command).
# Third-party packages are installed from uv.lock in a layer of their own before the source is copied, so a code change
# rebuilds only the small layers at the end, and uv's download cache lives in a BuildKit cache mount instead of the
# image. The services run as an unprivileged user (uid 10001) who can read the code but not change it.
FROM python:3.12-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/app/.venv/bin:$PATH \
    SAFESTART_DATA_ROOT=/data

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --all-extras --no-install-project

COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src

# /state is where the watcher keeps its state (a named volume takes its owner from this directory); /data is the
# mount point of the data root, read-only in every service but the orchestrator, which writes it as uid 10001.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --all-extras \
    && python -m compileall -q src \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin safestart \
    && install -d /data \
    && install -d -o safestart -g safestart /state
COPY scripts/smoke_test.py ./scripts/smoke_test.py

USER safestart
# A writable home as the working directory: Streamlit and Prefect keep their settings there, and Prefect the database
# of its local server when no Prefect API is configured.
WORKDIR /home/safestart
EXPOSE 8501
CMD ["streamlit", "run", "/app/src/safestart_pipeline/dashboard/app.py"]
