from flask import Flask
from src.models import init_db
from src.api import product_bp
from src.utils import register_error_handlers, setup_logging
from src.config import Config

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    setup_logging()
    
    init_db()
    
    app.register_blueprint(product_bp)
    
    register_error_handlers(app)
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=Config.FLASK_PORT, debug=True)