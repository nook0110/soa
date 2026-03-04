from flask_sqlalchemy import SQLAlchemy
from src.config import Config

db = SQLAlchemy()

def init_db():
    from flask import Flask
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = Config.SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = Config.SQLALCHEMY_TRACK_MODIFICATIONS
    
    db.init_app(app)
    
    with app.app_context():
        db.create_all()
    
    return app