FROM python:3.13-slim AS builder

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update && apt-get install -y git curl gcc libpq-dev make

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_PROJECT_ENVIRONMENT=/usr/local/ \
    UV_PYTHON=/usr/local/bin/python \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_FROZEN=1


WORKDIR /workspace

# Install third party dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
      uv sync --all-extras --frozen --no-dev --no-install-project

COPY . /workspace
RUN --mount=type=cache,target=/root/.cache/uv \
      uv pip install -e .

CMD ["sh", "-c", "prefect work-pool create --overwrite --set-as-default --type nersc nersc; prefect worker start --type nersc --pool nersc"]
