FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt grpcio-tools==1.64.0

# Proto file is copied from repo root via docker-compose build context
COPY proto/movie_event.proto /proto/movie_event.proto
RUN python -m grpc_tools.protoc \
        --proto_path=/proto \
        --python_out=/app \
        /proto/movie_event.proto

COPY producer/app/ ./app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
