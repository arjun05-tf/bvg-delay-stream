# Producer image. Also used for the tools, which import the producer package.
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first, so a source change does not re-resolve the world.
COPY producer/pyproject.toml /app/producer/pyproject.toml
RUN pip install --no-cache-dir \
        "requests>=2.32,<3" \
        "protobuf>=5.27,<7" \
        "gtfs-realtime-bindings>=1.0.0,<2" \
        "confluent-kafka[protobuf,schemaregistry]>=2.5,<3" \
        "prometheus-client>=0.20,<1" \
        "psycopg[binary]>=3.2,<4" \
        "grpcio-tools>=1.60"

COPY proto /app/proto
COPY producer/src /app/producer/src
COPY tools /app/tools

# Generate the protobuf bindings from the single source of truth in /app/proto.
RUN mkdir -p /app/producer/src/bvg_producer/generated/delay/v1 \
 && python -m grpc_tools.protoc -I/app/proto \
        --python_out=/app/producer/src/bvg_producer/generated \
        --pyi_out=/app/producer/src/bvg_producer/generated \
        /app/proto/delay/v1/stop_event.proto \
 && for dir in /app/producer/src/bvg_producer/generated \
               /app/producer/src/bvg_producer/generated/delay \
               /app/producer/src/bvg_producer/generated/delay/v1; do \
        echo '"""Generated protobuf modules."""' > "$dir/__init__.py"; \
    done

ENV PYTHONPATH=/app/producer/src:/app/tools

# Never run as root: a container that only needs to read a public feed and
# write to Kafka has no business holding uid 0.
RUN useradd --create-home --uid 10001 appuser \
 && mkdir -p /snapshots && chown appuser:appuser /snapshots
USER appuser

CMD ["python", "-m", "bvg_producer"]
