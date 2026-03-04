import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://products_user:products_pass@localhost:5432/products_db')
    AUTH_GRPC_HOST = os.getenv('AUTH_GRPC_HOST', 'auth-service:50051')
    AUTH_SERVICE_URL = os.getenv('AUTH_SERVICE_URL', 'auth-service:50051')
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5002))
    GRPC_PORT = int(os.getenv('GRPC_PORT', 50052))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')