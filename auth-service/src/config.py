import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://auth_user:auth_pass@localhost:5432/auth_db')
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'dev-secret-key-change-in-production')
    JWT_ACCESS_TOKEN_EXPIRES = int(os.getenv('JWT_ACCESS_TOKEN_EXPIRES', 900))
    JWT_REFRESH_TOKEN_EXPIRES = int(os.getenv('JWT_REFRESH_TOKEN_EXPIRES', 604800))
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5001))
    GRPC_PORT = int(os.getenv('GRPC_PORT', 50051))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')