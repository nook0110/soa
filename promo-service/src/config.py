import os

class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', 'postgresql://promo_user:promo_pass@localhost:5432/promo_db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    AUTH_GRPC_HOST = os.getenv('AUTH_GRPC_HOST', 'localhost:50051')
    
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5004))
    GRPC_PORT = int(os.getenv('GRPC_PORT', 50054))
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')