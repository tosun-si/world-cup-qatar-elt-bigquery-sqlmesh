FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY sqlmesh_ui/pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

FROM python:3.11-slim AS runtime

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY macros/ /app/macros/
COPY models/ /app/models/
COPY seeds/ /app/seeds/
COPY config.yaml /app/config.yaml

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

ENTRYPOINT ["sqlmesh", "ui", "--host", "0.0.0.0", "--port", "8000"]
