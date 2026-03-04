import threading
from flask import Flask
from flask_cors import CORS
from src.config import Config
from src.api.auth_routes import auth_bp
from src.middleware.logging_middleware import setup_logging_middleware
from src.utils.errors import register_error_handlers
from src.utils.logger import logger
from src.grpc_server.auth_server import serve as serve_grpc

def create_app():
    app = Flask(__name__)
    
    CORS(app)
    
    setup_logging_middleware(app)
    
    register_error_handlers(app)
    
    app.register_blueprint(auth_bp)
    
    @app.route('/health', methods=['GET'])
    def health():
        return {'status': 'healthy', 'service': 'auth-service'}, 200
    
    return app

def main():
    app = create_app()
    
    grpc_thread = threading.Thread(
        target=lambda: serve_grpc(Config.GRPC_PORT).wait_for_termination(),
        daemon=True
    )
    grpc_thread.start()
    
    logger.info(
        "starting_auth_service",
        flask_port=Config.FLASK_PORT,
        grpc_port=Config.GRPC_PORT,
        environment=Config.ENVIRONMENT
    )
    
    app.run(
        host='0.0.0.0',
        port=Config.FLASK_PORT,
        debug=(Config.ENVIRONMENT == 'development')
    )

if __name__ == '__main__':
    main()