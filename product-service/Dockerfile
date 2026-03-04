FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY product-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files
COPY proto /app/proto

# Copy application code
COPY product-service /app

# Generate gRPC code
RUN mkdir -p src/grpc_generated && \
    python -m grpc_tools.protoc \
    -I/app/proto \
    --python_out=./src/grpc_generated \
    --grpc_python_out=./src/grpc_generated \
    /app/proto/*.proto

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Expose ports
EXPOSE 5002 50052

# Run migrations and start application
CMD ["sh", "-c", "alembic upgrade head && python -m src.app"]